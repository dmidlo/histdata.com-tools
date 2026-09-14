"""Exact semantics for cross-family official economic indicators.

Retail, production, trade, housing, and official confidence/survey statistics
share immutable value, window, revision, and release-package mechanics without
collapsing their economic meaning.  Proprietary-only concepts are represented
as explicit unsupported gaps and can never produce official observations.
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
from typing import Any, TypeVar, cast
from urllib.parse import urlparse

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import (
    EconomicEventFamily,
    EconomicReleaseStage,
)
from histdatacom.market_context.official_sources import (
    OfficialSourceRegistryV1,
    OfficialSourceRole,
)
from histdatacom.runtime_contracts import JSONValue

ACTIVITY_WINDOW_SCHEMA_VERSION = "histdatacom.activity-window.v1"
ACTIVITY_CONCEPT_SCHEMA_VERSION = "histdatacom.activity-concept.v1"
ACTIVITY_UNSUPPORTED_GAP_SCHEMA_VERSION = (
    "histdatacom.activity-unsupported-gap.v1"
)
ACTIVITY_OBSERVATION_SCHEMA_VERSION = "histdatacom.activity-observation.v1"
ACTIVITY_VINTAGE_CHAIN_SCHEMA_VERSION = "histdatacom.activity-vintage-chain.v1"
ACTIVITY_EXPECTATION_SCHEMA_VERSION = "histdatacom.activity-expectation.v1"
ACTIVITY_RELEASE_TRIPLET_SCHEMA_VERSION = (
    "histdatacom.activity-release-triplet.v1"
)
ACTIVITY_RELEASE_SCHEMA_VERSION = "histdatacom.activity-release.v1"
ACTIVITY_RELEASE_PACKAGE_SCHEMA_VERSION = (
    "histdatacom.activity-release-package.v1"
)
ACTIVITY_SOURCE_OWNER_SCHEMA_VERSION = "histdatacom.activity-source-owner.v1"
ACTIVITY_ECONOMY_PROFILE_SCHEMA_VERSION = (
    "histdatacom.activity-economy-profile.v1"
)
ACTIVITY_PROFILE_AUDIT_SCHEMA_VERSION = "histdatacom.activity-profile-audit.v1"

MAX_ACTIVITY_REVISIONS = 10_000
MAX_ACTIVITY_RELEASE_OBSERVATIONS = 10_000
MAX_ACTIVITY_PACKAGE_RELEASES = 128
MAX_ACTIVITY_PROFILE_OWNERS = 32

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_ECONOMY_RE = re.compile(r"^[A-Z][A-Z0-9-]{1,7}$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EnumT = TypeVar("_EnumT", bound=Enum)


class ActivityIndicatorFamily(str, Enum):
    """Exact professional-calendar numeric family."""

    RETAIL_SALES = "retail-sales"
    HOUSEHOLD_CONSUMPTION = "household-consumption"
    INDUSTRIAL_PRODUCTION = "industrial-production"
    MANUFACTURING_PRODUCTION = "manufacturing-production"
    CONSTRUCTION_OUTPUT = "construction-output"
    DURABLE_GOODS_ORDERS = "durable-goods-orders"
    CAPITAL_GOODS_ORDERS = "capital-goods-orders"
    IMPORTS = "imports"
    EXPORTS = "exports"
    TRADE_BALANCE = "trade-balance"
    CURRENT_ACCOUNT = "current-account"
    HOUSING_STARTS = "housing-starts"
    BUILDING_PERMITS = "building-permits"
    HOME_SALES = "home-sales"
    HOUSE_PRICES = "house-prices"
    CONSUMER_CONFIDENCE = "consumer-confidence"
    BUSINESS_CONFIDENCE = "business-confidence"
    OFFICIAL_DIFFUSION_SURVEY = "official-diffusion-survey"

    @classmethod
    def from_value(
        cls, value: str | ActivityIndicatorFamily
    ) -> ActivityIndicatorFamily:
        return _enum_value(cls, value, "activity indicator family")

    @property
    def event_family(self) -> EconomicEventFamily:
        if self in {
            ActivityIndicatorFamily.RETAIL_SALES,
            ActivityIndicatorFamily.HOUSEHOLD_CONSUMPTION,
        }:
            return EconomicEventFamily.RETAIL_CONSUMPTION
        if self in {
            ActivityIndicatorFamily.INDUSTRIAL_PRODUCTION,
            ActivityIndicatorFamily.MANUFACTURING_PRODUCTION,
            ActivityIndicatorFamily.CONSTRUCTION_OUTPUT,
            ActivityIndicatorFamily.DURABLE_GOODS_ORDERS,
            ActivityIndicatorFamily.CAPITAL_GOODS_ORDERS,
        }:
            return EconomicEventFamily.INDUSTRIAL_PRODUCTION
        if self in {
            ActivityIndicatorFamily.IMPORTS,
            ActivityIndicatorFamily.EXPORTS,
            ActivityIndicatorFamily.TRADE_BALANCE,
            ActivityIndicatorFamily.CURRENT_ACCOUNT,
        }:
            return EconomicEventFamily.TRADE_EXTERNAL
        if self in {
            ActivityIndicatorFamily.HOUSING_STARTS,
            ActivityIndicatorFamily.BUILDING_PERMITS,
            ActivityIndicatorFamily.HOME_SALES,
            ActivityIndicatorFamily.HOUSE_PRICES,
        }:
            return EconomicEventFamily.HOUSING
        return EconomicEventFamily.CONFIDENCE_SURVEY


class ActivityStatisticKind(str, Enum):
    """Economic quantity measured before applying a transformation."""

    VALUE = "value"
    VOLUME = "volume"
    COUNT = "count"
    BALANCE = "balance"
    ORDERS = "orders"
    SALES = "sales"
    PRICE = "price"
    CONFIDENCE_INDEX = "confidence-index"
    DIFFUSION_INDEX = "diffusion-index"
    NET_BALANCE = "net-balance"

    @classmethod
    def from_value(
        cls, value: str | ActivityStatisticKind
    ) -> ActivityStatisticKind:
        return _enum_value(cls, value, "activity statistic kind")


class ActivityVariant(str, Enum):
    """Headline, component, or producer-defined sub-aggregate."""

    HEADLINE = "headline"
    COMPONENT = "component"
    EX_AUTOMOTIVE = "excluding-automotive"
    EX_FUEL = "excluding-fuel"
    CORE_CAPITAL_GOODS = "core-capital-goods"
    DOMESTIC = "domestic"
    FOREIGN = "foreign"
    NEW = "new"
    EXISTING = "existing"
    OFFICIAL_SURVEY_AGGREGATE = "official-survey-aggregate"

    @classmethod
    def from_value(cls, value: str | ActivityVariant) -> ActivityVariant:
        return _enum_value(cls, value, "activity variant")


class ActivityTransformation(str, Enum):
    """Exact representation displayed by the official producer."""

    LEVEL = "level"
    INDEX_LEVEL = "index-level"
    BALANCE_LEVEL = "balance-level"
    ABSOLUTE_CHANGE = "absolute-change"
    PERIOD_CHANGE = "period-change"
    MONTH_OVER_MONTH_PERCENT = "month-over-month-percent"
    QUARTER_OVER_QUARTER_PERCENT = "quarter-over-quarter-percent"
    YEAR_OVER_YEAR_PERCENT = "year-over-year-percent"
    THREE_MONTH_OVER_THREE_MONTH_PERCENT = (
        "three-month-over-three-month-percent"
    )
    ROLLING_SUM = "rolling-sum"
    CUMULATIVE_SUM = "cumulative-sum"
    DIFFUSION_INDEX_LEVEL = "diffusion-index-level"
    NET_BALANCE_PERCENT = "net-balance-percent"

    @classmethod
    def from_value(
        cls, value: str | ActivityTransformation
    ) -> ActivityTransformation:
        return _enum_value(cls, value, "activity transformation")


class ActivityFrequency(str, Enum):
    """Reference-period frequency."""

    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"

    @classmethod
    def from_value(cls, value: str | ActivityFrequency) -> ActivityFrequency:
        return _enum_value(cls, value, "activity frequency")


class ActivitySeasonalBasis(str, Enum):
    """Seasonal/calendar adjustment applied by the producer."""

    NOT_SEASONALLY_ADJUSTED = "not-seasonally-adjusted"
    SEASONALLY_ADJUSTED = "seasonally-adjusted"
    SEASONALLY_AND_CALENDAR_ADJUSTED = "seasonally-and-calendar-adjusted"
    CALENDAR_ADJUSTED = "calendar-adjusted"
    SEASONALLY_ADJUSTED_ANNUAL_RATE = "seasonally-adjusted-annual-rate"
    TREND_CYCLE = "trend-cycle"
    NOT_APPLICABLE = "not-applicable"

    @classmethod
    def from_value(
        cls, value: str | ActivitySeasonalBasis
    ) -> ActivitySeasonalBasis:
        return _enum_value(cls, value, "activity seasonal basis")


class ActivityUnitKind(str, Enum):
    """Semantic unit class; the lexical unit remains separately retained."""

    CURRENCY = "currency"
    COUNT = "count"
    AREA = "area"
    INDEX_POINTS = "index-points"
    VOLUME_INDEX = "volume-index"
    DIFFUSION_INDEX_POINTS = "diffusion-index-points"
    PERCENT = "percent"
    PERCENTAGE_POINTS = "percentage-points"

    @classmethod
    def from_value(cls, value: str | ActivityUnitKind) -> ActivityUnitKind:
        return _enum_value(cls, value, "activity unit kind")


class ActivityWindowKind(str, Enum):
    """Exact aggregation/comparison window of the displayed statistic."""

    SINGLE_PERIOD = "single-period"
    TRAILING = "trailing"
    CUMULATIVE_CALENDAR_YEAR = "cumulative-calendar-year"
    CUMULATIVE_FISCAL_YEAR = "cumulative-fiscal-year"
    THREE_MONTH_OVER_THREE_MONTH = "three-month-over-three-month"

    @classmethod
    def from_value(cls, value: str | ActivityWindowKind) -> ActivityWindowKind:
        return _enum_value(cls, value, "activity window kind")


class ActivityRevisionKind(str, Enum):
    """Why an official observation supersedes its preceding vintage."""

    INITIAL = "initial"
    ROUTINE = "routine-revision"
    SIMULTANEOUS_PREVIOUS = "simultaneous-previous-revision"
    BENCHMARK = "benchmark-revision"
    METHODOLOGY = "methodology-revision"
    SEASONAL = "seasonal-reanalysis"

    @classmethod
    def from_value(
        cls, value: str | ActivityRevisionKind
    ) -> ActivityRevisionKind:
        return _enum_value(cls, value, "activity revision kind")


class ActivityExpectationKind(str, Enum):
    """Provenance and target scope of an activity expectation."""

    EVENT_CONSENSUS = "event-consensus"
    OFFICIAL_SURVEY_EVENT_TARGET = "official-survey-event-target"
    OFFICIAL_SURVEY_PERIOD_TARGET = "official-survey-period-target"
    GOVERNMENT_PROJECTION = "government-projection"
    MACHINE_EVENT_TARGET = "machine-event-target"
    UNAVAILABLE = "unavailable"

    @classmethod
    def from_value(
        cls, value: str | ActivityExpectationKind
    ) -> ActivityExpectationKind:
        return _enum_value(cls, value, "activity expectation kind")

    @property
    def calendar_style_eligible(self) -> bool:
        return self in {
            ActivityExpectationKind.EVENT_CONSENSUS,
            ActivityExpectationKind.OFFICIAL_SURVEY_EVENT_TARGET,
            ActivityExpectationKind.MACHINE_EVENT_TARGET,
        }


class ActivityUnsupportedReason(str, Enum):
    """Why a calendar concept cannot have an official observation."""

    PROPRIETARY_SOURCE_UNQUALIFIED = "proprietary-source-unqualified"
    NO_OFFICIAL_EQUIVALENT = "no-official-equivalent"
    SOURCE_UNAVAILABLE = "source-unavailable"

    @classmethod
    def from_value(
        cls, value: str | ActivityUnsupportedReason
    ) -> ActivityUnsupportedReason:
        return _enum_value(cls, value, "unsupported activity reason")


def _enum_value(
    enum_type: type[_EnumT], value: str | _EnumT, label: str
) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value))
    except ValueError as exc:
        raise ValueError(f"unsupported {label}") from exc


def _required_text(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be text")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{label} must not be empty")
    return normalized


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("optional text must be text")
    normalized = value.strip()
    return normalized or None


def _key(value: object, label: str) -> str:
    normalized = _required_text(value, label)
    if _KEY_RE.fullmatch(normalized) is None:
        raise ValueError(f"{label} is not a canonical key")
    return normalized


def _optional_key(value: object, label: str) -> str | None:
    normalized = _optional_text(value)
    return None if normalized is None else _key(normalized, label)


def _economy(value: object) -> str:
    normalized = _required_text(value, "economy_code").upper()
    if _ECONOMY_RE.fullmatch(normalized) is None:
        raise ValueError("economy_code is invalid")
    return normalized


def _currency(value: object) -> str:
    normalized = _required_text(value, "currency_code").upper()
    if _CURRENCY_RE.fullmatch(normalized) is None:
        raise ValueError("currency_code must be ISO-like uppercase text")
    return normalized


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite")
    return result


def _optional_finite(value: object, label: str) -> float | None:
    return None if value is None else _finite(value, label)


def _bounded_ns(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    if value < 0 or value > 9_223_372_036_854_775_807:
        raise ValueError(f"{label} is outside int64 nanoseconds")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    if value < 0:
        raise ValueError(f"{label} must be nonnegative")
    return value


def _sha256(value: object, label: str) -> str:
    normalized = _required_text(value, label).lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return normalized


def _https_uri(value: object, label: str) -> str:
    normalized = _required_text(value, label)
    parsed = urlparse(normalized)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ValueError(f"{label} must be an absolute HTTPS URI")
    return normalized


def _texts(
    values: Iterable[object],
    label: str,
    *,
    required: bool = False,
    maximum: int = MAX_ACTIVITY_RELEASE_OBSERVATIONS,
) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item, label) for item in values}))
    if required and not result:
        raise ValueError(f"{label} must not be empty")
    if len(result) > maximum:
        raise ValueError(f"{label} exceeds item bound")
    return result


def _keys(
    values: Iterable[object],
    label: str,
    *,
    required: bool = False,
    maximum: int = MAX_ACTIVITY_RELEASE_OBSERVATIONS,
) -> tuple[str, ...]:
    result = tuple(sorted({_key(item, label) for item in values}))
    if required and not result:
        raise ValueError(f"{label} must not be empty")
    if len(result) > maximum:
        raise ValueError(f"{label} exceeds item bound")
    return result


def _mapping(value: object, label: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    return value


def _sequence(value: object, label: str = "sequence") -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{label} must be a sequence")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _month_index(timestamp_ns: int) -> int:
    moment = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, timezone.utc)
    return moment.year * 12 + moment.month - 1


@dataclass(frozen=True, slots=True)
class ActivityWindowV1:
    """Exact aggregation/comparison window retained by an activity concept."""

    kind: ActivityWindowKind
    periods: int
    anchor: str | None
    overlapping: bool
    window_id: str = ""
    schema_version: str = ACTIVITY_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_WINDOW_SCHEMA_VERSION:
            raise ValueError("unsupported activity window schema")
        kind = ActivityWindowKind.from_value(self.kind)
        periods = _nonnegative_int(self.periods, "periods")
        anchor = _optional_text(self.anchor)
        if not isinstance(self.overlapping, bool):
            raise TypeError("overlapping must be boolean")
        if kind is ActivityWindowKind.SINGLE_PERIOD:
            if periods != 1 or anchor is not None or self.overlapping:
                raise ValueError("single-period window is inconsistent")
        elif kind is ActivityWindowKind.TRAILING:
            if periods < 2 or anchor is not None or not self.overlapping:
                raise ValueError("trailing window is inconsistent")
        elif kind is ActivityWindowKind.THREE_MONTH_OVER_THREE_MONTH:
            if periods != 3 or anchor is not None or not self.overlapping:
                raise ValueError("three-month window is inconsistent")
        elif periods < 1 or anchor is None:
            raise ValueError("cumulative window requires periods and anchor")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "periods", periods)
        object.__setattr__(self, "anchor", anchor)
        expected = _stable_id("activity-window", self.identity_payload())
        supplied = _optional_text(self.window_id)
        if supplied is not None and supplied != expected:
            raise ValueError("window_id differs from deterministic identity")
        object.__setattr__(self, "window_id", expected)

    @classmethod
    def single_period(cls) -> ActivityWindowV1:
        return cls(
            kind=ActivityWindowKind.SINGLE_PERIOD,
            periods=1,
            anchor=None,
            overlapping=False,
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "periods": self.periods,
            "anchor": self.anchor,
            "overlapping": self.overlapping,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "window_id": self.window_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivityWindowV1:
        return cls(
            kind=ActivityWindowKind.from_value(str(data.get("kind", ""))),
            periods=cast(int, data.get("periods")),
            anchor=_optional_text(data.get("anchor")),
            overlapping=cast(bool, data.get("overlapping")),
            window_id=str(data.get("window_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ActivityConceptV1:
    """One officially supported source concept and exact transformation."""

    economy_code: str
    indicator_key: str
    source_series_id: str
    official_program: str
    family: ActivityIndicatorFamily
    statistic_kind: ActivityStatisticKind
    variant: ActivityVariant
    transformation: ActivityTransformation
    frequency: ActivityFrequency
    seasonal_basis: ActivitySeasonalBasis
    unit_kind: ActivityUnitKind
    unit: str
    scale: float
    currency_code: str | None
    component_path: tuple[str, ...]
    window: ActivityWindowV1
    producer_defines_percentage: bool
    methodology_era_id: str
    concept_id: str = ""
    schema_version: str = ACTIVITY_CONCEPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_CONCEPT_SCHEMA_VERSION:
            raise ValueError("unsupported activity concept schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        for name in ("indicator_key", "source_series_id", "methodology_era_id"):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        object.__setattr__(
            self,
            "official_program",
            _required_text(self.official_program, "official_program"),
        )
        family = ActivityIndicatorFamily.from_value(self.family)
        statistic = ActivityStatisticKind.from_value(self.statistic_kind)
        variant = ActivityVariant.from_value(self.variant)
        transformation = ActivityTransformation.from_value(self.transformation)
        frequency = ActivityFrequency.from_value(self.frequency)
        seasonal = ActivitySeasonalBasis.from_value(self.seasonal_basis)
        unit_kind = ActivityUnitKind.from_value(self.unit_kind)
        path = _keys(self.component_path, "component_path")
        if variant is ActivityVariant.COMPONENT and not path:
            raise ValueError("component variant requires component path")
        if variant is ActivityVariant.HEADLINE and path:
            raise ValueError("headline variant cannot claim component path")
        if (
            transformation is ActivityTransformation.MONTH_OVER_MONTH_PERCENT
            and (frequency is not ActivityFrequency.MONTHLY)
        ):
            raise ValueError("MoM transformation requires monthly frequency")
        if (
            transformation
            is ActivityTransformation.QUARTER_OVER_QUARTER_PERCENT
            and frequency is not ActivityFrequency.QUARTERLY
        ):
            raise ValueError("QoQ transformation requires quarterly frequency")
        percent_transformations = {
            ActivityTransformation.MONTH_OVER_MONTH_PERCENT,
            ActivityTransformation.QUARTER_OVER_QUARTER_PERCENT,
            ActivityTransformation.YEAR_OVER_YEAR_PERCENT,
            ActivityTransformation.THREE_MONTH_OVER_THREE_MONTH_PERCENT,
            ActivityTransformation.NET_BALANCE_PERCENT,
        }
        if transformation in percent_transformations and unit_kind is not (
            ActivityUnitKind.PERCENT
        ):
            raise ValueError("percentage transformation requires percent unit")
        if (
            transformation is ActivityTransformation.INDEX_LEVEL
            and unit_kind
            not in {
                ActivityUnitKind.INDEX_POINTS,
                ActivityUnitKind.VOLUME_INDEX,
            }
        ):
            raise ValueError("index level requires an index unit")
        if (
            transformation is ActivityTransformation.DIFFUSION_INDEX_LEVEL
            or statistic is ActivityStatisticKind.DIFFUSION_INDEX
        ) and (
            transformation is not ActivityTransformation.DIFFUSION_INDEX_LEVEL
            or statistic is not ActivityStatisticKind.DIFFUSION_INDEX
            or unit_kind is not ActivityUnitKind.DIFFUSION_INDEX_POINTS
        ):
            raise ValueError(
                "diffusion statistic requires diffusion-index identity"
            )
        if (
            transformation is ActivityTransformation.NET_BALANCE_PERCENT
            or statistic is ActivityStatisticKind.NET_BALANCE
        ) and (
            transformation is not ActivityTransformation.NET_BALANCE_PERCENT
            or statistic is not ActivityStatisticKind.NET_BALANCE
            or unit_kind is not ActivityUnitKind.PERCENT
            or not self.producer_defines_percentage
        ):
            raise ValueError("net-balance percent requires producer definition")
        if not isinstance(self.producer_defines_percentage, bool):
            raise TypeError("producer_defines_percentage must be boolean")
        if self.producer_defines_percentage and transformation is not (
            ActivityTransformation.NET_BALANCE_PERCENT
        ):
            raise ValueError("percentage-definition flag is not applicable")
        if (
            family
            in {
                ActivityIndicatorFamily.TRADE_BALANCE,
                ActivityIndicatorFamily.CURRENT_ACCOUNT,
            }
            and statistic is not ActivityStatisticKind.BALANCE
        ):
            raise ValueError(
                "external balance family requires balance statistic"
            )
        if (
            statistic is ActivityStatisticKind.BALANCE
            and transformation
            not in {
                ActivityTransformation.BALANCE_LEVEL,
                ActivityTransformation.ABSOLUTE_CHANGE,
                ActivityTransformation.MONTH_OVER_MONTH_PERCENT,
                ActivityTransformation.QUARTER_OVER_QUARTER_PERCENT,
                ActivityTransformation.YEAR_OVER_YEAR_PERCENT,
            }
        ):
            raise ValueError("balance statistic has invalid transformation")
        if not isinstance(self.window, ActivityWindowV1):
            raise TypeError("activity concept requires an exact window")
        if transformation is ActivityTransformation.ROLLING_SUM and (
            self.window.kind is not ActivityWindowKind.TRAILING
        ):
            raise ValueError("rolling sum requires trailing window")
        if transformation is ActivityTransformation.CUMULATIVE_SUM and (
            self.window.kind
            not in {
                ActivityWindowKind.CUMULATIVE_CALENDAR_YEAR,
                ActivityWindowKind.CUMULATIVE_FISCAL_YEAR,
            }
        ):
            raise ValueError("cumulative sum requires cumulative window")
        if transformation is (
            ActivityTransformation.THREE_MONTH_OVER_THREE_MONTH_PERCENT
        ) and self.window.kind is not (
            ActivityWindowKind.THREE_MONTH_OVER_THREE_MONTH
        ):
            raise ValueError("three-month transformation requires exact window")
        if (
            transformation
            not in {
                ActivityTransformation.ROLLING_SUM,
                ActivityTransformation.CUMULATIVE_SUM,
                ActivityTransformation.THREE_MONTH_OVER_THREE_MONTH_PERCENT,
            }
            and self.window.kind is not ActivityWindowKind.SINGLE_PERIOD
        ):
            raise ValueError(
                "single-period transformation has aggregate window"
            )
        currency = (
            None
            if self.currency_code is None
            else _currency(self.currency_code)
        )
        if (unit_kind is ActivityUnitKind.CURRENCY) != (currency is not None):
            raise ValueError("currency unit and currency code must agree")
        scale = _finite(self.scale, "scale")
        if scale <= 0:
            raise ValueError("scale must be positive")
        object.__setattr__(self, "family", family)
        object.__setattr__(self, "statistic_kind", statistic)
        object.__setattr__(self, "variant", variant)
        object.__setattr__(self, "transformation", transformation)
        object.__setattr__(self, "frequency", frequency)
        object.__setattr__(self, "seasonal_basis", seasonal)
        object.__setattr__(self, "unit_kind", unit_kind)
        object.__setattr__(self, "unit", _required_text(self.unit, "unit"))
        object.__setattr__(self, "scale", scale)
        object.__setattr__(self, "currency_code", currency)
        object.__setattr__(self, "component_path", path)
        expected = _stable_id("activity-concept", self.identity_payload())
        supplied = _optional_text(self.concept_id)
        if supplied is not None and supplied != expected:
            raise ValueError("concept_id differs from deterministic identity")
        object.__setattr__(self, "concept_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "indicator_key": self.indicator_key,
            "source_series_id": self.source_series_id,
            "official_program": self.official_program,
            "family": self.family.value,
            "statistic_kind": self.statistic_kind.value,
            "variant": self.variant.value,
            "transformation": self.transformation.value,
            "frequency": self.frequency.value,
            "seasonal_basis": self.seasonal_basis.value,
            "unit_kind": self.unit_kind.value,
            "unit": self.unit,
            "scale": self.scale,
            "currency_code": self.currency_code,
            "component_path": list(self.component_path),
            "window": self.window.to_dict(),
            "producer_defines_percentage": self.producer_defines_percentage,
            "methodology_era_id": self.methodology_era_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "concept_id": self.concept_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivityConceptV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            indicator_key=str(data.get("indicator_key", "")),
            source_series_id=str(data.get("source_series_id", "")),
            official_program=str(data.get("official_program", "")),
            family=ActivityIndicatorFamily.from_value(
                str(data.get("family", ""))
            ),
            statistic_kind=ActivityStatisticKind.from_value(
                str(data.get("statistic_kind", ""))
            ),
            variant=ActivityVariant.from_value(str(data.get("variant", ""))),
            transformation=ActivityTransformation.from_value(
                str(data.get("transformation", ""))
            ),
            frequency=ActivityFrequency.from_value(
                str(data.get("frequency", ""))
            ),
            seasonal_basis=ActivitySeasonalBasis.from_value(
                str(data.get("seasonal_basis", ""))
            ),
            unit_kind=ActivityUnitKind.from_value(
                str(data.get("unit_kind", ""))
            ),
            unit=str(data.get("unit", "")),
            scale=cast(float, data.get("scale")),
            currency_code=_optional_text(data.get("currency_code")),
            component_path=tuple(
                str(item)
                for item in _sequence(
                    data.get("component_path"), "component_path"
                )
            ),
            window=ActivityWindowV1.from_dict(
                _mapping(data.get("window"), "window")
            ),
            producer_defines_percentage=cast(
                bool, data.get("producer_defines_percentage")
            ),
            methodology_era_id=str(data.get("methodology_era_id", "")),
            concept_id=str(data.get("concept_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ActivityConceptV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("activity concept is invalid JSON") from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class UnsupportedActivityGapV1:
    """Explicit gap for an unqualified proprietary or unavailable concept."""

    economy_code: str
    indicator_key: str
    family: ActivityIndicatorFamily
    requested_label: str
    producer_name: str | None
    reason: ActivityUnsupportedReason
    official_replacement_concept_id: str | None
    notes: tuple[str, ...]
    gap_id: str = ""
    schema_version: str = ACTIVITY_UNSUPPORTED_GAP_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_UNSUPPORTED_GAP_SCHEMA_VERSION:
            raise ValueError("unsupported activity-gap schema")
        object.__setattr__(self, "economy_code", _economy(self.economy_code))
        object.__setattr__(
            self, "indicator_key", _key(self.indicator_key, "indicator_key")
        )
        object.__setattr__(
            self, "family", ActivityIndicatorFamily.from_value(self.family)
        )
        object.__setattr__(
            self,
            "requested_label",
            _required_text(self.requested_label, "requested_label"),
        )
        producer = _optional_text(self.producer_name)
        reason = ActivityUnsupportedReason.from_value(self.reason)
        if (
            reason is ActivityUnsupportedReason.PROPRIETARY_SOURCE_UNQUALIFIED
            and producer is None
        ):
            raise ValueError("proprietary gap requires producer identity")
        replacement = _optional_text(self.official_replacement_concept_id)
        if replacement is not None and replacement == self.indicator_key:
            raise ValueError(
                "official alternative must remain a distinct concept"
            )
        object.__setattr__(self, "producer_name", producer)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "official_replacement_concept_id", replacement)
        object.__setattr__(
            self, "notes", _texts(self.notes, "note", required=True)
        )
        expected = _stable_id(
            "activity-unsupported-gap", self.identity_payload()
        )
        supplied = _optional_text(self.gap_id)
        if supplied is not None and supplied != expected:
            raise ValueError("gap_id differs from deterministic identity")
        object.__setattr__(self, "gap_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "indicator_key": self.indicator_key,
            "family": self.family.value,
            "requested_label": self.requested_label,
            "producer_name": self.producer_name,
            "reason": self.reason.value,
            "official_replacement_concept_id": (
                self.official_replacement_concept_id
            ),
            "notes": list(self.notes),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "gap_id": self.gap_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> UnsupportedActivityGapV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            indicator_key=str(data.get("indicator_key", "")),
            family=ActivityIndicatorFamily.from_value(
                str(data.get("family", ""))
            ),
            requested_label=str(data.get("requested_label", "")),
            producer_name=_optional_text(data.get("producer_name")),
            reason=ActivityUnsupportedReason.from_value(
                str(data.get("reason", ""))
            ),
            official_replacement_concept_id=_optional_text(
                data.get("official_replacement_concept_id")
            ),
            notes=tuple(
                str(item) for item in _sequence(data.get("notes"), "notes")
            ),
            gap_id=str(data.get("gap_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ActivityObservationV1:
    """One immutable official value vintage for an activity concept."""

    concept: ActivityConceptV1
    logical_event_key: str
    reference_period: str
    reference_period_end_ns: int
    stage: EconomicReleaseStage
    value: float
    raw_lexical: str
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_release_id: str
    source_request_id: str
    content_sha256: str
    revision_sequence: int
    revision_kind: ActivityRevisionKind
    supersedes_observation_id: str | None
    limitations: tuple[str, ...]
    observation_id: str = ""
    schema_version: str = ACTIVITY_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_OBSERVATION_SCHEMA_VERSION:
            raise ValueError("unsupported activity observation schema")
        if not isinstance(self.concept, ActivityConceptV1):
            raise TypeError("observation requires an official activity concept")
        for name in (
            "logical_event_key",
            "source_key",
            "source_release_id",
            "source_request_id",
        ):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        object.__setattr__(
            self,
            "reference_period",
            _required_text(self.reference_period, "reference_period"),
        )
        object.__setattr__(
            self,
            "reference_period_end_ns",
            _bounded_ns(
                self.reference_period_end_ns, "reference_period_end_ns"
            ),
        )
        object.__setattr__(
            self, "stage", EconomicReleaseStage.from_value(self.stage)
        )
        object.__setattr__(self, "value", _finite(self.value, "value"))
        object.__setattr__(
            self, "raw_lexical", _required_text(self.raw_lexical, "raw_lexical")
        )
        published = _bounded_ns(self.published_at_ns, "published_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if available < published:
            raise ValueError("observation availability precedes publication")
        object.__setattr__(self, "published_at_ns", published)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        revision = _nonnegative_int(self.revision_sequence, "revision_sequence")
        if revision > MAX_ACTIVITY_REVISIONS:
            raise ValueError("revision_sequence exceeds bound")
        kind = ActivityRevisionKind.from_value(self.revision_kind)
        supersedes = _optional_text(self.supersedes_observation_id)
        if revision == 0:
            if (
                kind is not ActivityRevisionKind.INITIAL
                or supersedes is not None
            ):
                raise ValueError("initial observation has revision evidence")
        elif kind is ActivityRevisionKind.INITIAL or supersedes is None:
            raise ValueError("revision requires kind and predecessor")
        object.__setattr__(self, "revision_sequence", revision)
        object.__setattr__(self, "revision_kind", kind)
        object.__setattr__(self, "supersedes_observation_id", supersedes)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id("activity-observation", self.identity_payload())
        supplied = _optional_text(self.observation_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "observation_id differs from deterministic identity"
            )
        object.__setattr__(self, "observation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "concept": self.concept.to_dict(),
            "logical_event_key": self.logical_event_key,
            "reference_period": self.reference_period,
            "reference_period_end_ns": self.reference_period_end_ns,
            "stage": self.stage.value,
            "value": self.value,
            "raw_lexical": self.raw_lexical,
            "published_at_ns": self.published_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_release_id": self.source_release_id,
            "source_request_id": self.source_request_id,
            "content_sha256": self.content_sha256,
            "revision_sequence": self.revision_sequence,
            "revision_kind": self.revision_kind.value,
            "supersedes_observation_id": self.supersedes_observation_id,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "observation_id": self.observation_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivityObservationV1:
        return cls(
            concept=ActivityConceptV1.from_dict(
                _mapping(data.get("concept"), "concept")
            ),
            logical_event_key=str(data.get("logical_event_key", "")),
            reference_period=str(data.get("reference_period", "")),
            reference_period_end_ns=cast(
                int, data.get("reference_period_end_ns")
            ),
            stage=EconomicReleaseStage.from_value(str(data.get("stage", ""))),
            value=cast(float, data.get("value")),
            raw_lexical=str(data.get("raw_lexical", "")),
            published_at_ns=cast(int, data.get("published_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_key=str(data.get("source_key", "")),
            source_release_id=str(data.get("source_release_id", "")),
            source_request_id=str(data.get("source_request_id", "")),
            content_sha256=str(data.get("content_sha256", "")),
            revision_sequence=cast(int, data.get("revision_sequence")),
            revision_kind=ActivityRevisionKind.from_value(
                str(data.get("revision_kind", ""))
            ),
            supersedes_observation_id=_optional_text(
                data.get("supersedes_observation_id")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            observation_id=str(data.get("observation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ActivityVintageChainV1:
    """Initial official value and every later revision without overwrite."""

    observations: tuple[ActivityObservationV1, ...]
    chain_id: str = ""
    schema_version: str = ACTIVITY_VINTAGE_CHAIN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_VINTAGE_CHAIN_SCHEMA_VERSION:
            raise ValueError("unsupported activity vintage-chain schema")
        observations = tuple(
            sorted(self.observations, key=lambda item: item.revision_sequence)
        )
        if not observations or len(observations) > MAX_ACTIVITY_REVISIONS + 1:
            raise ValueError("activity vintage-chain size is invalid")
        if any(
            not isinstance(item, ActivityObservationV1) for item in observations
        ):
            raise TypeError("vintage chain contains a non-v1 observation")
        first = observations[0]
        common = (
            first.concept.concept_id,
            first.logical_event_key,
            first.reference_period,
            first.reference_period_end_ns,
            first.stage,
        )
        for position, observation in enumerate(observations):
            if observation.revision_sequence != position:
                raise ValueError("vintage chain revision sequence has a gap")
            if (
                observation.concept.concept_id,
                observation.logical_event_key,
                observation.reference_period,
                observation.reference_period_end_ns,
                observation.stage,
            ) != common:
                raise ValueError("vintage chain changes event or concept")
            if position and observation.supersedes_observation_id != (
                observations[position - 1].observation_id
            ):
                raise ValueError("revision does not supersede its predecessor")
            if position and observation.available_at_ns < (
                observations[position - 1].available_at_ns
            ):
                raise ValueError("revision availability moves backward")
        object.__setattr__(self, "observations", observations)
        expected = _stable_id("activity-vintage-chain", self.identity_payload())
        supplied = _optional_text(self.chain_id)
        if supplied is not None and supplied != expected:
            raise ValueError("chain_id differs from deterministic identity")
        object.__setattr__(self, "chain_id", expected)

    @property
    def initial(self) -> ActivityObservationV1:
        return self.observations[0]

    def as_known_at(self, decision_at_ns: int) -> ActivityObservationV1 | None:
        cutoff = _bounded_ns(decision_at_ns, "decision_at_ns")
        visible = [
            item for item in self.observations if item.available_at_ns <= cutoff
        ]
        return visible[-1] if visible else None

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "observations": [item.to_dict() for item in self.observations],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "chain_id": self.chain_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivityVintageChainV1:
        return cls(
            observations=tuple(
                ActivityObservationV1.from_dict(_mapping(item, "observation"))
                for item in _sequence(data.get("observations"), "observations")
            ),
            chain_id=str(data.get("chain_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ActivityExpectationV1:
    """Pre-release expectation for one exact official activity event."""

    target_event_key: str
    concept: ActivityConceptV1
    reference_period: str
    kind: ActivityExpectationKind
    expected_value: float | None
    collection_start_ns: int | None
    collection_cutoff_ns: int | None
    available_at_ns: int | None
    source_key: str | None
    source_snapshot_id: str | None
    source_uri: str | None
    unavailable_reason: str | None
    limitations: tuple[str, ...]
    expectation_id: str = ""
    schema_version: str = ACTIVITY_EXPECTATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_EXPECTATION_SCHEMA_VERSION:
            raise ValueError("unsupported activity expectation schema")
        object.__setattr__(
            self,
            "target_event_key",
            _key(self.target_event_key, "target_event_key"),
        )
        if not isinstance(self.concept, ActivityConceptV1):
            raise TypeError("expectation requires an activity concept")
        object.__setattr__(
            self,
            "reference_period",
            _required_text(self.reference_period, "reference_period"),
        )
        kind = ActivityExpectationKind.from_value(self.kind)
        expected_value = _optional_finite(self.expected_value, "expected_value")
        start = (
            None
            if self.collection_start_ns is None
            else _bounded_ns(self.collection_start_ns, "collection_start_ns")
        )
        cutoff = (
            None
            if self.collection_cutoff_ns is None
            else _bounded_ns(self.collection_cutoff_ns, "collection_cutoff_ns")
        )
        available = (
            None
            if self.available_at_ns is None
            else _bounded_ns(self.available_at_ns, "available_at_ns")
        )
        source_key = _optional_key(self.source_key, "source_key")
        snapshot = _optional_text(self.source_snapshot_id)
        uri = _optional_text(self.source_uri)
        reason = _optional_text(self.unavailable_reason)
        if kind is ActivityExpectationKind.UNAVAILABLE:
            if expected_value is not None or any(
                item is not None
                for item in (
                    start,
                    cutoff,
                    available,
                    source_key,
                    snapshot,
                    uri,
                )
            ):
                raise ValueError("unavailable expectation contains evidence")
            if reason is None:
                raise ValueError("unavailable expectation requires a reason")
        else:
            if expected_value is None:
                raise ValueError("observed expectation requires a value")
            if any(item is None for item in (start, cutoff, available)):
                raise ValueError(
                    "observed expectation requires collection times"
                )
            assert (
                start is not None
                and cutoff is not None
                and available is not None
            )
            if not start <= cutoff <= available:
                raise ValueError("expectation collection chronology is invalid")
            if source_key is None or snapshot is None or uri is None:
                raise ValueError(
                    "observed expectation requires source evidence"
                )
            uri = _https_uri(uri, "source_uri")
            if reason is not None:
                raise ValueError("observed expectation cannot be unavailable")
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "expected_value", expected_value)
        object.__setattr__(self, "collection_start_ns", start)
        object.__setattr__(self, "collection_cutoff_ns", cutoff)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "source_key", source_key)
        object.__setattr__(self, "source_snapshot_id", snapshot)
        object.__setattr__(self, "source_uri", uri)
        object.__setattr__(self, "unavailable_reason", reason)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id("activity-expectation", self.identity_payload())
        supplied = _optional_text(self.expectation_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "expectation_id differs from deterministic identity"
            )
        object.__setattr__(self, "expectation_id", expected)

    @property
    def calendar_style_eligible(self) -> bool:
        return self.kind.calendar_style_eligible

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "target_event_key": self.target_event_key,
            "concept": self.concept.to_dict(),
            "reference_period": self.reference_period,
            "kind": self.kind.value,
            "expected_value": self.expected_value,
            "collection_start_ns": self.collection_start_ns,
            "collection_cutoff_ns": self.collection_cutoff_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_snapshot_id": self.source_snapshot_id,
            "source_uri": self.source_uri,
            "unavailable_reason": self.unavailable_reason,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "expectation_id": self.expectation_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivityExpectationV1:
        return cls(
            target_event_key=str(data.get("target_event_key", "")),
            concept=ActivityConceptV1.from_dict(
                _mapping(data.get("concept"), "concept")
            ),
            reference_period=str(data.get("reference_period", "")),
            kind=ActivityExpectationKind.from_value(str(data.get("kind", ""))),
            expected_value=cast(float | None, data.get("expected_value")),
            collection_start_ns=cast(
                int | None, data.get("collection_start_ns")
            ),
            collection_cutoff_ns=cast(
                int | None, data.get("collection_cutoff_ns")
            ),
            available_at_ns=cast(int | None, data.get("available_at_ns")),
            source_key=_optional_text(data.get("source_key")),
            source_snapshot_id=_optional_text(data.get("source_snapshot_id")),
            source_uri=_optional_text(data.get("source_uri")),
            unavailable_reason=_optional_text(data.get("unavailable_reason")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            expectation_id=str(data.get("expectation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _is_immediately_preceding(
    previous_ns: int, current_ns: int, frequency: ActivityFrequency
) -> bool:
    if frequency is ActivityFrequency.WEEKLY:
        return current_ns - previous_ns == 7 * 86_400 * 1_000_000_000
    lag = _month_index(current_ns) - _month_index(previous_ns)
    return (
        lag
        == {
            ActivityFrequency.MONTHLY: 1,
            ActivityFrequency.QUARTERLY: 3,
            ActivityFrequency.ANNUAL: 12,
        }[frequency]
    )


@dataclass(frozen=True, slots=True)
class ActivityReleaseTripletV1:
    """Initial actual, previous-as-known, and exact-concept forecast."""

    target_event_key: str
    actual_initial: ActivityObservationV1
    previous_as_known: ActivityObservationV1
    previous_as_known_at_ns: int
    expectation: ActivityExpectationV1
    triplet_id: str = ""
    schema_version: str = ACTIVITY_RELEASE_TRIPLET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_RELEASE_TRIPLET_SCHEMA_VERSION:
            raise ValueError("unsupported activity release-triplet schema")
        target = _key(self.target_event_key, "target_event_key")
        if not isinstance(
            self.actual_initial, ActivityObservationV1
        ) or not isinstance(self.previous_as_known, ActivityObservationV1):
            raise TypeError("triplet actuals must use activity observations")
        if not isinstance(self.expectation, ActivityExpectationV1):
            raise TypeError("triplet expectation must use the v1 contract")
        actual = self.actual_initial
        previous = self.previous_as_known
        if actual.logical_event_key != target:
            raise ValueError("initial actual belongs to another event")
        if actual.revision_sequence != 0:
            raise ValueError("triplet actual must be first published")
        if actual.concept.concept_id != previous.concept.concept_id:
            raise ValueError("previous field silently changes activity concept")
        if not _is_immediately_preceding(
            previous.reference_period_end_ns,
            actual.reference_period_end_ns,
            actual.concept.frequency,
        ):
            raise ValueError(
                "previous field is not immediately preceding period"
            )
        if previous.logical_event_key == target:
            raise ValueError("previous field reuses the current event")
        cutoff = _bounded_ns(
            self.previous_as_known_at_ns, "previous_as_known_at_ns"
        )
        if previous.available_at_ns > cutoff or cutoff > actual.published_at_ns:
            raise ValueError(
                "previous-as-known evidence is not release-bounded"
            )
        if (
            previous.revision_kind is ActivityRevisionKind.SIMULTANEOUS_PREVIOUS
            and previous.available_at_ns != actual.published_at_ns
        ):
            raise ValueError(
                "simultaneous previous revision has wrong timestamp"
            )
        expectation = self.expectation
        if (
            expectation.target_event_key != target
            or expectation.concept.concept_id != actual.concept.concept_id
            or expectation.reference_period != actual.reference_period
        ):
            raise ValueError("forecast does not match exact released concept")
        if expectation.kind is not ActivityExpectationKind.UNAVAILABLE:
            if not expectation.calendar_style_eligible:
                raise ValueError("period projection cannot populate forecast")
            if (
                expectation.available_at_ns is None
                or expectation.available_at_ns >= actual.published_at_ns
            ):
                raise ValueError("forecast was not available before release")
        object.__setattr__(self, "target_event_key", target)
        object.__setattr__(self, "previous_as_known_at_ns", cutoff)
        expected = _stable_id(
            "activity-release-triplet", self.identity_payload()
        )
        supplied = _optional_text(self.triplet_id)
        if supplied is not None and supplied != expected:
            raise ValueError("triplet_id differs from deterministic identity")
        object.__setattr__(self, "triplet_id", expected)

    @property
    def actual(self) -> float:
        return self.actual_initial.value

    @property
    def previous(self) -> float:
        return self.previous_as_known.value

    @property
    def forecast(self) -> float | None:
        return self.expectation.expected_value

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "target_event_key": self.target_event_key,
            "actual_initial": self.actual_initial.to_dict(),
            "previous_as_known": self.previous_as_known.to_dict(),
            "previous_as_known_at_ns": self.previous_as_known_at_ns,
            "expectation": self.expectation.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "triplet_id": self.triplet_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivityReleaseTripletV1:
        return cls(
            target_event_key=str(data.get("target_event_key", "")),
            actual_initial=ActivityObservationV1.from_dict(
                _mapping(data.get("actual_initial"), "actual_initial")
            ),
            previous_as_known=ActivityObservationV1.from_dict(
                _mapping(data.get("previous_as_known"), "previous_as_known")
            ),
            previous_as_known_at_ns=cast(
                int, data.get("previous_as_known_at_ns")
            ),
            expectation=ActivityExpectationV1.from_dict(
                _mapping(data.get("expectation"), "expectation")
            ),
            triplet_id=str(data.get("triplet_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ActivityReleaseV1:
    """One independently meaningful event inside an official package."""

    package_key: str
    logical_event_key: str
    family: ActivityIndicatorFamily
    economy_code: str
    reference_period: str
    reference_period_end_ns: int
    stage: EconomicReleaseStage
    published_at_ns: int
    available_at_ns: int
    source_key: str
    source_release_id: str
    source_request_id: str
    content_sha256: str
    observations: tuple[ActivityObservationV1, ...]
    display_observation_id: str
    limitations: tuple[str, ...]
    release_id: str = ""
    schema_version: str = ACTIVITY_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported activity release schema")
        package_key = _key(self.package_key, "package_key")
        event_key = _key(self.logical_event_key, "logical_event_key")
        family = ActivityIndicatorFamily.from_value(self.family)
        economy = _economy(self.economy_code)
        period = _required_text(self.reference_period, "reference_period")
        period_end = _bounded_ns(
            self.reference_period_end_ns, "reference_period_end_ns"
        )
        stage = EconomicReleaseStage.from_value(self.stage)
        published = _bounded_ns(self.published_at_ns, "published_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if available < published:
            raise ValueError("release availability precedes publication")
        source_key = _key(self.source_key, "source_key")
        source_release = _key(self.source_release_id, "source_release_id")
        source_request = _key(self.source_request_id, "source_request_id")
        digest = _sha256(self.content_sha256, "content_sha256")
        observations = tuple(
            sorted(self.observations, key=lambda item: item.observation_id)
        )
        if (
            not observations
            or len(observations) > MAX_ACTIVITY_RELEASE_OBSERVATIONS
            or any(
                not isinstance(item, ActivityObservationV1)
                for item in observations
            )
        ):
            raise ValueError("activity release observation set is invalid")
        if len({item.concept.concept_id for item in observations}) != len(
            observations
        ):
            raise ValueError("release repeats an activity concept")
        for observation in observations:
            if (
                observation.logical_event_key,
                observation.concept.economy_code,
                observation.concept.family,
                observation.reference_period,
                observation.reference_period_end_ns,
                observation.stage,
                observation.published_at_ns,
                observation.available_at_ns,
                observation.source_key,
                observation.source_release_id,
                observation.source_request_id,
                observation.content_sha256,
            ) != (
                event_key,
                economy,
                family,
                period,
                period_end,
                stage,
                published,
                available,
                source_key,
                source_release,
                source_request,
                digest,
            ):
                raise ValueError(
                    "release mixes event, concept, or source evidence"
                )
            if observation.revision_sequence != 0:
                raise ValueError(
                    "release contains a later observation revision"
                )
        display = _required_text(
            self.display_observation_id, "display_observation_id"
        )
        if display not in {item.observation_id for item in observations}:
            raise ValueError("display observation is not retained by release")
        object.__setattr__(self, "package_key", package_key)
        object.__setattr__(self, "logical_event_key", event_key)
        object.__setattr__(self, "family", family)
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "reference_period_end_ns", period_end)
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "published_at_ns", published)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "source_key", source_key)
        object.__setattr__(self, "source_release_id", source_release)
        object.__setattr__(self, "source_request_id", source_request)
        object.__setattr__(self, "content_sha256", digest)
        object.__setattr__(self, "observations", observations)
        object.__setattr__(self, "display_observation_id", display)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id("activity-release", self.identity_payload())
        supplied = _optional_text(self.release_id)
        if supplied is not None and supplied != expected:
            raise ValueError("release_id differs from deterministic identity")
        object.__setattr__(self, "release_id", expected)

    @property
    def display_observation(self) -> ActivityObservationV1:
        return next(
            item
            for item in self.observations
            if item.observation_id == self.display_observation_id
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "package_key": self.package_key,
            "logical_event_key": self.logical_event_key,
            "family": self.family.value,
            "economy_code": self.economy_code,
            "reference_period": self.reference_period,
            "reference_period_end_ns": self.reference_period_end_ns,
            "stage": self.stage.value,
            "published_at_ns": self.published_at_ns,
            "available_at_ns": self.available_at_ns,
            "source_key": self.source_key,
            "source_release_id": self.source_release_id,
            "source_request_id": self.source_request_id,
            "content_sha256": self.content_sha256,
            "observations": [item.to_dict() for item in self.observations],
            "display_observation_id": self.display_observation_id,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivityReleaseV1:
        return cls(
            package_key=str(data.get("package_key", "")),
            logical_event_key=str(data.get("logical_event_key", "")),
            family=ActivityIndicatorFamily.from_value(
                str(data.get("family", ""))
            ),
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            reference_period_end_ns=cast(
                int, data.get("reference_period_end_ns")
            ),
            stage=EconomicReleaseStage.from_value(str(data.get("stage", ""))),
            published_at_ns=cast(int, data.get("published_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            source_key=str(data.get("source_key", "")),
            source_release_id=str(data.get("source_release_id", "")),
            source_request_id=str(data.get("source_request_id", "")),
            content_sha256=str(data.get("content_sha256", "")),
            observations=tuple(
                ActivityObservationV1.from_dict(_mapping(item, "observation"))
                for item in _sequence(data.get("observations"), "observations")
            ),
            display_observation_id=str(data.get("display_observation_id", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            release_id=str(data.get("release_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ActivityReleasePackageV1:
    """Distinct indicator events from one official publication package."""

    package_key: str
    releases: tuple[ActivityReleaseV1, ...]
    limitations: tuple[str, ...]
    package_id: str = ""
    schema_version: str = ACTIVITY_RELEASE_PACKAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_RELEASE_PACKAGE_SCHEMA_VERSION:
            raise ValueError("unsupported activity release-package schema")
        package_key = _key(self.package_key, "package_key")
        releases = tuple(
            sorted(self.releases, key=lambda item: item.logical_event_key)
        )
        if (
            not releases
            or len(releases) > MAX_ACTIVITY_PACKAGE_RELEASES
            or any(not isinstance(item, ActivityReleaseV1) for item in releases)
        ):
            raise ValueError("activity release package is outside bounds")
        if any(item.package_key != package_key for item in releases):
            raise ValueError("release package key does not match members")
        if len({item.logical_event_key for item in releases}) != len(releases):
            raise ValueError("release package collapses distinct events")
        if len(
            {item.display_observation.concept.concept_id for item in releases}
        ) != len(releases):
            raise ValueError("release package repeats a displayed concept")
        first = releases[0]
        evidence = (
            first.economy_code,
            first.published_at_ns,
            first.available_at_ns,
            first.source_key,
            first.source_release_id,
            first.source_request_id,
            first.content_sha256,
        )
        for release in releases:
            if (
                release.economy_code,
                release.published_at_ns,
                release.available_at_ns,
                release.source_key,
                release.source_release_id,
                release.source_request_id,
                release.content_sha256,
            ) != evidence:
                raise ValueError("release package mixes publication evidence")
        object.__setattr__(self, "package_key", package_key)
        object.__setattr__(self, "releases", releases)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitation", required=True),
        )
        expected = _stable_id(
            "activity-release-package", self.identity_payload()
        )
        supplied = _optional_text(self.package_id)
        if supplied is not None and supplied != expected:
            raise ValueError("package_id differs from deterministic identity")
        object.__setattr__(self, "package_id", expected)

    def release_for_family(
        self, family: ActivityIndicatorFamily
    ) -> ActivityReleaseV1:
        selected = ActivityIndicatorFamily.from_value(family)
        matches = [item for item in self.releases if item.family is selected]
        if len(matches) != 1:
            raise ValueError("package has no unique release for family")
        return matches[0]

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "package_key": self.package_key,
            "releases": [item.to_dict() for item in self.releases],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "package_id": self.package_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivityReleasePackageV1:
        return cls(
            package_key=str(data.get("package_key", "")),
            releases=tuple(
                ActivityReleaseV1.from_dict(_mapping(item, "release"))
                for item in _sequence(data.get("releases"), "releases")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            package_id=str(data.get("package_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


_PROFILE_EVENT_FAMILIES: tuple[EconomicEventFamily, ...] = (
    EconomicEventFamily.RETAIL_CONSUMPTION,
    EconomicEventFamily.INDUSTRIAL_PRODUCTION,
    EconomicEventFamily.TRADE_EXTERNAL,
    EconomicEventFamily.HOUSING,
    EconomicEventFamily.CONFIDENCE_SURVEY,
)


@dataclass(frozen=True, slots=True)
class ActivitySourceOwnerV1:
    """Legal producer selected for one broad activity event family."""

    event_family: EconomicEventFamily
    source_key: str
    institution: str
    owner_id: str = ""
    schema_version: str = ACTIVITY_SOURCE_OWNER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_SOURCE_OWNER_SCHEMA_VERSION:
            raise ValueError("unsupported activity source-owner schema")
        family = EconomicEventFamily.from_value(self.event_family)
        if family not in _PROFILE_EVENT_FAMILIES:
            raise ValueError("source owner has unrelated event family")
        object.__setattr__(self, "event_family", family)
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        object.__setattr__(
            self, "institution", _required_text(self.institution, "institution")
        )
        expected = _stable_id("activity-source-owner", self.identity_payload())
        supplied = _optional_text(self.owner_id)
        if supplied is not None and supplied != expected:
            raise ValueError("owner_id differs from deterministic identity")
        object.__setattr__(self, "owner_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_family": self.event_family.value,
            "source_key": self.source_key,
            "institution": self.institution,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "owner_id": self.owner_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivitySourceOwnerV1:
        return cls(
            event_family=EconomicEventFamily.from_value(
                str(data.get("event_family", ""))
            ),
            source_key=str(data.get("source_key", "")),
            institution=str(data.get("institution", "")),
            owner_id=str(data.get("owner_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ActivityEconomyProfileV1:
    """Reviewed official owners and difficult-case rules for one economy."""

    economy_code: str
    source_owners: tuple[ActivitySourceOwnerV1, ...]
    required_indicator_families: tuple[ActivityIndicatorFamily, ...]
    proprietary_gaps: tuple[UnsupportedActivityGapV1, ...]
    supports_exact_windows: bool
    supports_release_packages: bool
    supports_simultaneous_previous_revisions: bool
    special_cases: tuple[str, ...]
    rationale: str
    profile_id: str = ""
    schema_version: str = ACTIVITY_ECONOMY_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_ECONOMY_PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported activity economy-profile schema")
        economy = _economy(self.economy_code)
        owners = tuple(
            sorted(self.source_owners, key=lambda item: item.event_family.value)
        )
        if len(owners) > MAX_ACTIVITY_PROFILE_OWNERS or any(
            not isinstance(item, ActivitySourceOwnerV1) for item in owners
        ):
            raise ValueError("profile source-owner set is invalid")
        if {item.event_family for item in owners} != set(
            _PROFILE_EVENT_FAMILIES
        ):
            raise ValueError(
                "profile must bind all activity event-family owners"
            )
        families = tuple(
            sorted(
                {
                    ActivityIndicatorFamily.from_value(item)
                    for item in self.required_indicator_families
                },
                key=lambda item: item.value,
            )
        )
        if set(families) != set(ActivityIndicatorFamily):
            raise ValueError("profile omits required activity indicator family")
        gaps = tuple(
            sorted(self.proprietary_gaps, key=lambda item: item.gap_id)
        )
        if not gaps or any(
            not isinstance(item, UnsupportedActivityGapV1) for item in gaps
        ):
            raise ValueError("profile requires explicit proprietary gaps")
        if any(
            item.economy_code != economy
            or item.reason
            is not ActivityUnsupportedReason.PROPRIETARY_SOURCE_UNQUALIFIED
            for item in gaps
        ):
            raise ValueError("profile proprietary gap is inconsistent")
        for name in (
            "supports_exact_windows",
            "supports_release_packages",
            "supports_simultaneous_previous_revisions",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        if not all(
            (
                self.supports_exact_windows,
                self.supports_release_packages,
                self.supports_simultaneous_previous_revisions,
            )
        ):
            raise ValueError("profile omits required cross-family mechanics")
        cases = _keys(self.special_cases, "special_case", required=True)
        required_cases = {
            "balance-value-versus-growth",
            "diffusion-index-versus-percent",
            "exact-cumulative-and-rolling-windows",
            "proprietary-survey-gaps",
            "release-package-identity",
            "simultaneous-previous-revisions",
        }
        if not required_cases.issubset(cases):
            raise ValueError("profile omits universal activity edge cases")
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "source_owners", owners)
        object.__setattr__(self, "required_indicator_families", families)
        object.__setattr__(self, "proprietary_gaps", gaps)
        object.__setattr__(self, "special_cases", cases)
        object.__setattr__(
            self, "rationale", _required_text(self.rationale, "rationale")
        )
        expected = _stable_id(
            "activity-economy-profile", self.identity_payload()
        )
        supplied = _optional_text(self.profile_id)
        if supplied is not None and supplied != expected:
            raise ValueError("profile_id differs from deterministic identity")
        object.__setattr__(self, "profile_id", expected)

    def owner_for(
        self, event_family: EconomicEventFamily
    ) -> ActivitySourceOwnerV1:
        selected = EconomicEventFamily.from_value(event_family)
        matches = [
            item for item in self.source_owners if item.event_family is selected
        ]
        if len(matches) != 1:
            raise ValueError("profile has no unique activity source owner")
        return matches[0]

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "source_owners": [item.to_dict() for item in self.source_owners],
            "required_indicator_families": [
                item.value for item in self.required_indicator_families
            ],
            "proprietary_gaps": [
                item.to_dict() for item in self.proprietary_gaps
            ],
            "supports_exact_windows": self.supports_exact_windows,
            "supports_release_packages": self.supports_release_packages,
            "supports_simultaneous_previous_revisions": (
                self.supports_simultaneous_previous_revisions
            ),
            "special_cases": list(self.special_cases),
            "rationale": self.rationale,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "profile_id": self.profile_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivityEconomyProfileV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            source_owners=tuple(
                ActivitySourceOwnerV1.from_dict(_mapping(item, "source owner"))
                for item in _sequence(
                    data.get("source_owners"), "source_owners"
                )
            ),
            required_indicator_families=tuple(
                ActivityIndicatorFamily.from_value(str(item))
                for item in _sequence(
                    data.get("required_indicator_families"),
                    "required_indicator_families",
                )
            ),
            proprietary_gaps=tuple(
                UnsupportedActivityGapV1.from_dict(
                    _mapping(item, "proprietary gap")
                )
                for item in _sequence(
                    data.get("proprietary_gaps"), "proprietary_gaps"
                )
            ),
            supports_exact_windows=cast(
                bool, data.get("supports_exact_windows")
            ),
            supports_release_packages=cast(
                bool, data.get("supports_release_packages")
            ),
            supports_simultaneous_previous_revisions=cast(
                bool, data.get("supports_simultaneous_previous_revisions")
            ),
            special_cases=tuple(
                str(item)
                for item in _sequence(
                    data.get("special_cases"), "special_cases"
                )
            ),
            rationale=str(data.get("rationale", "")),
            profile_id=str(data.get("profile_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


_UNIVERSAL_SPECIAL_CASES: tuple[str, ...] = (
    "balance-value-versus-growth",
    "component-and-headline-identity",
    "diffusion-index-versus-percent",
    "exact-cumulative-and-rolling-windows",
    "proprietary-survey-gaps",
    "release-package-identity",
    "seasonal-reanalysis",
    "simultaneous-previous-revisions",
)
_SPECIAL_CASES_BY_ECONOMY: Mapping[str, tuple[str, ...]] = {
    "EA": (
        "official-european-commission-tendency-surveys",
        "extra-eu-versus-intra-eu-trade",
    ),
    "GB": (
        "official-ons-output-and-trade",
        "private-pmi-and-confidence-boundary",
    ),
    "JP": ("official-boj-tankan-versus-private-pmi",),
    "US": (
        "advance-durable-and-core-capital-goods",
        "housing-starts-permits-and-sales-package",
        "private-pmi-and-confidence-boundary",
        "retail-control-group",
    ),
}


def built_in_activity_profiles(
    registry: OfficialSourceRegistryV1,
) -> tuple[ActivityEconomyProfileV1, ...]:
    """Build official ownership and semantic profiles for all economies."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("registry must use OfficialSourceRegistryV1")
    profiles: list[ActivityEconomyProfileV1] = []
    for economy_code in registry.scoped_economies:
        owners = tuple(
            ActivitySourceOwnerV1(
                event_family=family,
                source_key=source.source_key,
                institution=source.institution,
            )
            for family in _PROFILE_EVENT_FAMILIES
            for source in (registry.primary_source(economy_code, family),)
        )
        gap = UnsupportedActivityGapV1(
            economy_code=economy_code,
            indicator_key=f"{economy_code.lower()}-proprietary-pmi",
            family=ActivityIndicatorFamily.BUSINESS_CONFIDENCE,
            requested_label="Purchasing Managers Index",
            producer_name="unqualified proprietary survey producer",
            reason=ActivityUnsupportedReason.PROPRIETARY_SOURCE_UNQUALIFIED,
            official_replacement_concept_id=None,
            notes=(
                (
                    "No official-looking PMI substitute may be fabricated; an "
                    "official business survey remains a distinct concept."
                ),
            ),
        )
        profiles.append(
            ActivityEconomyProfileV1(
                economy_code=economy_code,
                source_owners=owners,
                required_indicator_families=tuple(ActivityIndicatorFamily),
                proprietary_gaps=(gap,),
                supports_exact_windows=True,
                supports_release_packages=True,
                supports_simultaneous_previous_revisions=True,
                special_cases=(
                    *_UNIVERSAL_SPECIAL_CASES,
                    *_SPECIAL_CASES_BY_ECONOMY.get(economy_code, ()),
                ),
                rationale=(
                    "Bind each activity family to its legal producer and "
                    "retain exact concept, transformation, unit, window, "
                    "vintage, and release-package identity. Availability is "
                    "a separate adapter qualification claim."
                ),
            )
        )
    return tuple(sorted(profiles, key=lambda item: item.economy_code))


@dataclass(frozen=True, slots=True)
class ActivityProfileAuditV1:
    """Coverage proof for owners, semantic families, and proprietary gaps."""

    registry_id: str
    profile_ids: tuple[str, ...]
    missing_economies: tuple[str, ...]
    duplicate_economies: tuple[str, ...]
    source_mismatches: tuple[str, ...]
    semantic_mismatches: tuple[str, ...]
    complete: bool
    audit_id: str = ""
    schema_version: str = ACTIVITY_PROFILE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ACTIVITY_PROFILE_AUDIT_SCHEMA_VERSION:
            raise ValueError("unsupported activity profile-audit schema")
        object.__setattr__(
            self, "registry_id", _required_text(self.registry_id, "registry_id")
        )
        object.__setattr__(
            self,
            "profile_ids",
            _texts(self.profile_ids, "profile_id", maximum=128),
        )
        for name in (
            "missing_economies",
            "duplicate_economies",
            "source_mismatches",
            "semantic_mismatches",
        ):
            object.__setattr__(
                self,
                name,
                _texts(getattr(self, name), name, maximum=512),
            )
        expected_complete = not any(
            (
                self.missing_economies,
                self.duplicate_economies,
                self.source_mismatches,
                self.semantic_mismatches,
            )
        )
        if (
            not isinstance(self.complete, bool)
            or self.complete != expected_complete
        ):
            raise ValueError(
                "activity profile audit completeness is inconsistent"
            )
        expected = _stable_id("activity-profile-audit", self.identity_payload())
        supplied = _optional_text(self.audit_id)
        if supplied is not None and supplied != expected:
            raise ValueError("audit_id differs from deterministic identity")
        object.__setattr__(self, "audit_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_ids": list(self.profile_ids),
            "missing_economies": list(self.missing_economies),
            "duplicate_economies": list(self.duplicate_economies),
            "source_mismatches": list(self.source_mismatches),
            "semantic_mismatches": list(self.semantic_mismatches),
            "complete": self.complete,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "audit_id": self.audit_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ActivityProfileAuditV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_ids=tuple(
                str(item)
                for item in _sequence(data.get("profile_ids"), "profile_ids")
            ),
            missing_economies=tuple(
                str(item)
                for item in _sequence(
                    data.get("missing_economies"), "missing_economies"
                )
            ),
            duplicate_economies=tuple(
                str(item)
                for item in _sequence(
                    data.get("duplicate_economies"), "duplicate_economies"
                )
            ),
            source_mismatches=tuple(
                str(item)
                for item in _sequence(
                    data.get("source_mismatches"), "source_mismatches"
                )
            ),
            semantic_mismatches=tuple(
                str(item)
                for item in _sequence(
                    data.get("semantic_mismatches"), "semantic_mismatches"
                )
            ),
            complete=cast(bool, data.get("complete")),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def audit_activity_profiles(
    registry: OfficialSourceRegistryV1,
    profiles: Iterable[ActivityEconomyProfileV1],
) -> ActivityProfileAuditV1:
    """Audit one complete profile and exact official owner per scope cell."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("registry must use OfficialSourceRegistryV1")
    values = tuple(profiles)
    if any(not isinstance(item, ActivityEconomyProfileV1) for item in values):
        raise TypeError("profiles must use ActivityEconomyProfileV1")
    counts = {
        code: sum(item.economy_code == code for item in values)
        for code in registry.scoped_economies
    }
    missing = tuple(code for code, count in counts.items() if count == 0)
    duplicates = tuple(code for code, count in counts.items() if count > 1)
    source_mismatches: list[str] = []
    semantic_mismatches: list[str] = []
    universal_cases = set(_UNIVERSAL_SPECIAL_CASES)
    for profile in values:
        code = profile.economy_code
        if code not in registry.scoped_economies:
            source_mismatches.append(f"{code}:outside-scope")
            continue
        for owner in profile.source_owners:
            expected = registry.primary_source(code, owner.event_family)
            try:
                source = registry.source(owner.source_key)
            except ValueError:
                source_mismatches.append(f"{code}:{owner.event_family.value}")
                continue
            if (
                owner.source_key != expected.source_key
                or source.economy_code != code
                or source.institution != owner.institution
                or OfficialSourceRole.PRIMARY_PRODUCER not in source.roles
            ):
                source_mismatches.append(f"{code}:{owner.event_family.value}")
        expected_cases = universal_cases | set(
            _SPECIAL_CASES_BY_ECONOMY.get(code, ())
        )
        if (
            set(profile.required_indicator_families)
            != set(ActivityIndicatorFamily)
            or not expected_cases.issubset(profile.special_cases)
            or len(profile.proprietary_gaps) != 1
            or profile.proprietary_gaps[0].economy_code != code
            or not all(
                (
                    profile.supports_exact_windows,
                    profile.supports_release_packages,
                    profile.supports_simultaneous_previous_revisions,
                )
            )
        ):
            semantic_mismatches.append(code)
    if len({item.profile_id for item in values}) != len(values):
        duplicates = tuple(sorted({*duplicates, "profile-id"}))
    source_result = tuple(sorted(set(source_mismatches)))
    semantic_result = tuple(sorted(set(semantic_mismatches)))
    return ActivityProfileAuditV1(
        registry_id=registry.registry_id,
        profile_ids=tuple(item.profile_id for item in values),
        missing_economies=missing,
        duplicate_economies=duplicates,
        source_mismatches=source_result,
        semantic_mismatches=semantic_result,
        complete=not any((missing, duplicates, source_result, semantic_result)),
    )


def require_activity_profile_coverage(
    registry: OfficialSourceRegistryV1,
    profiles: Iterable[ActivityEconomyProfileV1],
) -> ActivityProfileAuditV1:
    """Return a complete audit or fail closed on source/semantic gaps."""
    audit = audit_activity_profiles(registry, profiles)
    if not audit.complete:
        raise ValueError("activity profile coverage is incomplete")
    return audit


__all__ = [
    "ACTIVITY_CONCEPT_SCHEMA_VERSION",
    "ACTIVITY_ECONOMY_PROFILE_SCHEMA_VERSION",
    "ACTIVITY_EXPECTATION_SCHEMA_VERSION",
    "ACTIVITY_OBSERVATION_SCHEMA_VERSION",
    "ACTIVITY_PROFILE_AUDIT_SCHEMA_VERSION",
    "ACTIVITY_RELEASE_PACKAGE_SCHEMA_VERSION",
    "ACTIVITY_RELEASE_SCHEMA_VERSION",
    "ACTIVITY_RELEASE_TRIPLET_SCHEMA_VERSION",
    "ACTIVITY_SOURCE_OWNER_SCHEMA_VERSION",
    "ACTIVITY_UNSUPPORTED_GAP_SCHEMA_VERSION",
    "ACTIVITY_VINTAGE_CHAIN_SCHEMA_VERSION",
    "ACTIVITY_WINDOW_SCHEMA_VERSION",
    "MAX_ACTIVITY_PACKAGE_RELEASES",
    "MAX_ACTIVITY_PROFILE_OWNERS",
    "MAX_ACTIVITY_RELEASE_OBSERVATIONS",
    "MAX_ACTIVITY_REVISIONS",
    "ActivityConceptV1",
    "ActivityEconomyProfileV1",
    "ActivityExpectationKind",
    "ActivityExpectationV1",
    "ActivityFrequency",
    "ActivityIndicatorFamily",
    "ActivityObservationV1",
    "ActivityProfileAuditV1",
    "ActivityReleasePackageV1",
    "ActivityReleaseTripletV1",
    "ActivityReleaseV1",
    "ActivityRevisionKind",
    "ActivitySeasonalBasis",
    "ActivitySourceOwnerV1",
    "ActivityStatisticKind",
    "ActivityTransformation",
    "ActivityUnitKind",
    "ActivityUnsupportedReason",
    "ActivityVariant",
    "ActivityVintageChainV1",
    "ActivityWindowKind",
    "ActivityWindowV1",
    "UnsupportedActivityGapV1",
    "audit_activity_profiles",
    "built_in_activity_profiles",
    "require_activity_profile_coverage",
]
