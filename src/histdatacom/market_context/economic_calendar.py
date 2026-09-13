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
from typing import Any, Protocol, cast, runtime_checkable

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

MAX_ECONOMIC_CALENDAR_RELEASES = 100_000
MAX_ECONOMIC_CALENDAR_FORECASTS = 250_000
MAX_ECONOMIC_CALENDAR_QUERY_EVENTS = 512
MAX_ECONOMIC_CALENDAR_ADAPTERS = 64
MAX_ECONOMIC_CALENDAR_CORPUS_BYTES = 64 * 1024 * 1024

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9._:-]{3,32}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class EconomicReleaseStage(str, Enum):
    """Lifecycle stage proved by one immutable release vintage."""

    SCHEDULED = "scheduled"
    INITIAL = "initial"
    REVISION = "revision"
    CANCELLED = "cancelled"

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


class EconomicForecastKind(str, Enum):
    """Provenance class for an expectation available before a release."""

    OBSERVED_CONSENSUS = "observed_consensus"
    MACHINE_PROJECTION = "machine_projection"

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

    EVENT_RELEASE = "event_release"
    REFERENCE_PERIOD = "reference_period"

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


def _sha256(value: object, name: str) -> str:
    text = _required_text(value, name)
    if _SHA256_RE.fullmatch(text) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return text


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
class EconomicCalendarReleaseV1:
    """One immutable official release, schedule, cancellation, or revision."""

    logical_event_key: str
    series_key: str
    economy: str
    currency: str
    institution: str
    event_family: str
    indicator_id: str
    source_series_id: str
    title: str
    reference_period: str
    reference_period_end_ns: int
    frequency: str
    seasonality: str
    unit: str
    scale: float
    base: str | None
    stage: EconomicReleaseStage
    scheduled_for_ns: int
    released_at_ns: int | None
    first_observed_at_ns: int
    available_at_ns: int
    precision: MarketContextPrecision
    market_context_kind: MarketContextKind
    source: MarketContextSourceV1
    affected_currencies: tuple[str, ...]
    affected_symbols: tuple[str, ...]
    limitations: tuple[str, ...]
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
            "event_family",
            "indicator_id",
            "source_series_id",
        ):
            object.__setattr__(self, name, _key(getattr(self, name), name))
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
        scheduled = _bounded_ns(self.scheduled_for_ns, "scheduled_for_ns")
        released = _optional_ns(self.released_at_ns, "released_at_ns")
        first_observed = _bounded_ns(
            self.first_observed_at_ns, "first_observed_at_ns"
        )
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if first_observed > available:
            raise ValueError("first observed time follows availability")
        if not isinstance(self.source, MarketContextSourceV1):
            raise TypeError("source must use MarketContextSourceV1")
        if self.source.retrieved_at_ns < available:
            raise ValueError("source retrieval precedes release availability")
        if stage in {
            EconomicReleaseStage.INITIAL,
            EconomicReleaseStage.REVISION,
        }:
            if released is None:
                raise ValueError("an actual release requires released_at_ns")
            if self.actual_value is None:
                raise ValueError("an actual release requires actual_value")
        elif self.actual_value is not None or self.actual_lexical is not None:
            raise ValueError(
                "a schedule or cancellation cannot contain an actual"
            )
        object.__setattr__(self, "scheduled_for_ns", scheduled)
        object.__setattr__(self, "released_at_ns", released)
        object.__setattr__(self, "first_observed_at_ns", first_observed)
        object.__setattr__(self, "available_at_ns", available)
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
        """Return actual release time when known, otherwise scheduled time."""
        return self.released_at_ns or self.scheduled_for_ns

    def semantic_key(self) -> tuple[object, ...]:
        """Return fields that may not drift across vintages."""
        return (
            self.logical_event_key,
            self.series_key,
            self.economy,
            self.currency,
            self.institution,
            self.event_family,
            self.indicator_id,
            self.source_series_id,
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
            self.economy,
            self.currency,
            self.institution,
            self.event_family,
            self.indicator_id,
            self.source_series_id,
            self.frequency,
            self.seasonality,
            self.unit,
            self.scale,
            self.base,
            self.market_context_kind,
            self.affected_currencies,
            self.affected_symbols,
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic release identity."""
        return {
            "schema_version": self.schema_version,
            "logical_event_key": self.logical_event_key,
            "series_key": self.series_key,
            "economy": self.economy,
            "currency": self.currency,
            "institution": self.institution,
            "event_family": self.event_family,
            "indicator_id": self.indicator_id,
            "source_series_id": self.source_series_id,
            "title": self.title,
            "reference_period": self.reference_period,
            "reference_period_end_ns": self.reference_period_end_ns,
            "frequency": self.frequency,
            "seasonality": self.seasonality,
            "unit": self.unit,
            "scale": self.scale,
            "base": self.base,
            "stage": self.stage.value,
            "scheduled_for_ns": self.scheduled_for_ns,
            "released_at_ns": self.released_at_ns,
            "first_observed_at_ns": self.first_observed_at_ns,
            "available_at_ns": self.available_at_ns,
            "precision": self.precision.value,
            "market_context_kind": self.market_context_kind.value,
            "source": self.source.to_dict(),
            "affected_currencies": list(self.affected_currencies),
            "affected_symbols": list(self.affected_symbols),
            "limitations": list(self.limitations),
            "actual_value": self.actual_value,
            "actual_lexical": self.actual_lexical,
            "content_sha256": self.content_sha256,
            "revision_sequence": self.revision_sequence,
            "supersedes_release_id": self.supersedes_release_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible release metadata."""
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarReleaseV1:
        """Restore and verify one release vintage."""
        return cls(
            logical_event_key=str(data.get("logical_event_key", "")),
            series_key=str(data.get("series_key", "")),
            economy=str(data.get("economy", "")),
            currency=str(data.get("currency", "")),
            institution=str(data.get("institution", "")),
            event_family=str(data.get("event_family", "")),
            indicator_id=str(data.get("indicator_id", "")),
            source_series_id=str(data.get("source_series_id", "")),
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
            scheduled_for_ns=cast(int, data.get("scheduled_for_ns")),
            released_at_ns=cast(int | None, data.get("released_at_ns")),
            first_observed_at_ns=cast(int, data.get("first_observed_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
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


@dataclass(frozen=True, slots=True)
class EconomicCalendarForecastV1:
    """One observed-consensus or machine projection vintage."""

    logical_event_key: str
    kind: EconomicForecastKind
    scope: EconomicForecastScope
    produced_at_ns: int
    available_at_ns: int
    value: float
    lexical_value: str | None
    unit: str
    scale: float
    base: str | None
    source: MarketContextSourceV1
    limitations: tuple[str, ...]
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
        produced = _bounded_ns(self.produced_at_ns, "produced_at_ns")
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        if produced > available:
            raise ValueError("forecast production follows availability")
        if not isinstance(self.source, MarketContextSourceV1):
            raise TypeError("source must use MarketContextSourceV1")
        if self.source.retrieved_at_ns < available:
            raise ValueError("forecast source retrieval precedes availability")
        object.__setattr__(self, "produced_at_ns", produced)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "value", _finite(self.value, "value"))
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
            "produced_at_ns": self.produced_at_ns,
            "available_at_ns": self.available_at_ns,
            "value": self.value,
            "lexical_value": self.lexical_value,
            "unit": self.unit,
            "scale": self.scale,
            "base": self.base,
            "source": self.source.to_dict(),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible forecast metadata."""
        return {**self.identity_payload(), "forecast_id": self.forecast_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EconomicCalendarForecastV1:
        """Restore and verify one forecast vintage."""
        return cls(
            logical_event_key=str(data.get("logical_event_key", "")),
            kind=EconomicForecastKind.from_value(str(data.get("kind", ""))),
            scope=EconomicForecastScope.from_value(str(data.get("scope", ""))),
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
            forecast_id=str(data.get("forecast_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicCalendarEventStateV1:
    """Point-in-time state for one logical economic release."""

    release: EconomicCalendarReleaseV1
    visible_actual_vintages: tuple[EconomicCalendarReleaseV1, ...]
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
            or item.stage
            not in {EconomicReleaseStage.INITIAL, EconomicReleaseStage.REVISION}
            or item.available_at_ns > decision
            for item in actuals
        ):
            raise ValueError("visible actual vintages are inconsistent")
        actuals = tuple(
            sorted(actuals, key=lambda item: item.revision_sequence)
        )
        if actuals and actuals[0].stage is not EconomicReleaseStage.INITIAL:
            raise ValueError(
                "visible actual vintages require an initial release"
            )
        previous = self.previous_as_known
        if previous is not None:
            if previous.series_key != self.release.series_key:
                raise ValueError("previous-as-known belongs to another series")
            if (
                previous.reference_period_end_ns
                >= self.release.reference_period_end_ns
            ):
                raise ValueError("previous-as-known is not an earlier period")
            if previous.available_at_ns >= self.release.event_time_ns:
                raise ValueError(
                    "previous-as-known was not known before release"
                )
        for forecast in (self.observed_consensus, self.machine_projection):
            if forecast is None:
                continue
            if forecast.logical_event_key != self.release.logical_event_key:
                raise ValueError("forecast belongs to another logical event")
            if forecast.available_at_ns > decision:
                raise ValueError("state exposes a forecast after decision time")
            if forecast.available_at_ns >= self.release.event_time_ns:
                raise ValueError(
                    "forecast must be available strictly before release"
                )
            if forecast.unit != self.release.unit:
                raise ValueError("forecast unit differs from release unit")
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
    def previous_value(self) -> float | None:
        """Return ``P_e``, the prior period value known before release."""
        if self.previous_as_known is None:
            return None
        return self.previous_as_known.actual_value

    @property
    def observed_surprise(self) -> float | None:
        """Return first actual less independently observed consensus."""
        if self.actual_initial is None or self.observed_consensus is None:
            return None
        return self.actual_initial - self.observed_consensus.value

    @property
    def machine_surprise(self) -> float | None:
        """Return first actual less the point-in-time machine projection."""
        if self.actual_initial is None or self.machine_projection is None:
            return None
        return self.actual_initial - self.machine_projection.value

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic state identity."""
        return {
            "schema_version": self.schema_version,
            "release": self.release.to_dict(),
            "visible_actual_vintages": [
                item.to_dict() for item in self.visible_actual_vintages
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
            "actual_initial": self.actual_initial,
            "actual_latest": self.actual_latest,
            "previous_value": self.previous_value,
            "observed_surprise": self.observed_surprise,
            "machine_surprise": self.machine_surprise,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible state metadata."""
        return {**self.identity_payload(), "state_id": self.state_id}

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
        if any(
            not start <= item.release.event_time_ns < end for item in events
        ):
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
                        item.release.event_time_ns,
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
            tuple(
                sorted(
                    {
                        _key(item, "event_family")
                        for item in self.requested_event_families
                    }
                )
            ),
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
        by_series: dict[str, tuple[object, ...]] = {}
        for values in by_event.values():
            first = values[0]
            initial_actuals = 0
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
                if item.stage is EconomicReleaseStage.INITIAL:
                    initial_actuals += 1
            if initial_actuals > 1:
                raise ValueError(
                    "logical event contains multiple initial actuals"
                )
            series_semantics = first.series_semantic_key()
            prior_semantics = by_series.setdefault(
                first.series_key, series_semantics
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
        and item.scope is EconomicForecastScope.EVENT_RELEASE
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
    requested_families = tuple(
        sorted({_key(item, "event_family") for item in event_families})
    )
    releases_by_event: dict[str, list[EconomicCalendarReleaseV1]] = {}
    actuals_by_series: dict[str, list[EconomicCalendarReleaseV1]] = {}
    for release in corpus.releases:
        releases_by_event.setdefault(release.logical_event_key, []).append(
            release
        )
        if release.stage in {
            EconomicReleaseStage.INITIAL,
            EconomicReleaseStage.REVISION,
        }:
            actuals_by_series.setdefault(release.series_key, []).append(release)
    forecasts_by_event: dict[str, list[EconomicCalendarForecastV1]] = {}
    for forecast in corpus.forecasts:
        forecasts_by_event.setdefault(forecast.logical_event_key, []).append(
            forecast
        )
    latest_visible: list[EconomicCalendarReleaseV1] = []
    for values in releases_by_event.values():
        visible = [item for item in values if item.available_at_ns <= decision]
        if visible:
            latest_visible.append(
                max(visible, key=lambda item: item.revision_sequence)
            )
    candidates = [
        item
        for item in latest_visible
        if start <= item.event_time_ns < end
        and (
            not requested_currencies
            or bool(set(requested_currencies) & set(item.affected_currencies))
        )
        and (
            not requested_symbols
            or bool(set(requested_symbols) & set(item.affected_symbols))
        )
        and (not requested_families or item.event_family in requested_families)
    ]
    candidates.sort(
        key=lambda item: (item.event_time_ns, item.logical_event_key)
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
            if item.stage
            in {EconomicReleaseStage.INITIAL, EconomicReleaseStage.REVISION}
            and item.available_at_ns <= decision
        )
        previous_candidates = [
            item
            for item in actuals_by_series.get(release.series_key, ())
            if item.reference_period_end_ns < release.reference_period_end_ns
            and item.available_at_ns <= decision
            and item.available_at_ns < release.event_time_ns
        ]
        previous: EconomicCalendarReleaseV1 | None = None
        if previous_candidates:
            previous_period = max(
                item.reference_period_end_ns for item in previous_candidates
            )
            previous = max(
                (
                    item
                    for item in previous_candidates
                    if item.reference_period_end_ns == previous_period
                ),
                key=lambda item: (item.available_at_ns, item.revision_sequence),
            )
        forecasts = forecasts_by_event.get(release.logical_event_key, [])
        states.append(
            EconomicCalendarEventStateV1(
                release=release,
                visible_actual_vintages=actuals,
                previous_as_known=previous,
                observed_consensus=_latest_forecast(
                    forecasts,
                    kind=EconomicForecastKind.OBSERVED_CONSENSUS,
                    decision_at_ns=decision,
                    release_time_ns=release.event_time_ns,
                ),
                machine_projection=_latest_forecast(
                    forecasts,
                    kind=EconomicForecastKind.MACHINE_PROJECTION,
                    decision_at_ns=decision,
                    release_time_ns=release.event_time_ns,
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


def economic_release_from_market_context(
    event: MarketContextEventV1,
    *,
    logical_event_key: str,
    series_key: str,
    economy: str,
    currency: str,
    institution: str,
    event_family: str,
    indicator_id: str,
    source_series_id: str,
    reference_period: str,
    reference_period_end_ns: int,
    frequency: str,
    seasonality: str,
    unit: str,
    scale: float = 1.0,
    base: str | None = None,
    stage: EconomicReleaseStage,
    supersedes_release_id: str | None = None,
) -> EconomicCalendarReleaseV1:
    """Bridge an approved official market-context event into this contract."""
    if not isinstance(event, MarketContextEventV1):
        raise TypeError("bridge requires a MarketContextEventV1")
    selected_stage = EconomicReleaseStage.from_value(stage)
    return EconomicCalendarReleaseV1(
        logical_event_key=logical_event_key,
        series_key=series_key,
        economy=economy,
        currency=currency,
        institution=institution,
        event_family=event_family,
        indicator_id=indicator_id,
        source_series_id=source_series_id,
        title=event.title,
        reference_period=reference_period,
        reference_period_end_ns=reference_period_end_ns,
        frequency=frequency,
        seasonality=seasonality,
        unit=unit,
        scale=scale,
        base=base,
        stage=selected_stage,
        scheduled_for_ns=event.event_time_ns,
        released_at_ns=(
            event.event_time_ns
            if selected_stage
            in {EconomicReleaseStage.INITIAL, EconomicReleaseStage.REVISION}
            else None
        ),
        first_observed_at_ns=event.first_known_at_ns,
        available_at_ns=event.available_at_ns,
        precision=event.precision,
        market_context_kind=event.kind,
        source=event.source,
        affected_currencies=event.affected_currencies,
        affected_symbols=event.affected_symbols,
        limitations=event.limitations,
        actual_value=event.actual_value,
        actual_lexical=(
            None if event.actual_value is None else str(event.actual_value)
        ),
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
    return MarketContextEventV1(
        canonical_key=release.logical_event_key,
        kind=release.market_context_kind,
        title=release.title,
        source=release.source,
        source_event_time=_utc_text(release.event_time_ns),
        source_timezone="UTC",
        event_time_ns=release.event_time_ns,
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
            projection_tag,
        ),
    )


def _utc_text(timestamp_ns: int) -> str:
    if timestamp_ns % 1_000:
        raise ValueError(
            "MarketContextEventV1 projection requires microsecond-aligned time"
        )
    seconds, remainder = divmod(timestamp_ns, 1_000_000_000)
    base = datetime.fromtimestamp(seconds, tz=timezone.utc)
    if remainder:
        return (
            f"{base.strftime('%Y-%m-%dT%H:%M:%S')}."
            f"{remainder // 1_000:06d}+00:00"
        )
    return base.isoformat()


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
    "ECONOMIC_CALENDAR_CORPUS_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_FORECAST_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_QUERY_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_RELEASE_SCHEMA_VERSION",
    "ECONOMIC_CALENDAR_STATE_SCHEMA_VERSION",
    "MAX_ECONOMIC_CALENDAR_ADAPTERS",
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
    "EconomicForecastKind",
    "EconomicForecastScope",
    "EconomicReleaseStage",
    "StaticOfficialEconomicCalendarAdapterV1",
    "build_economic_calendar_corpus",
    "economic_release_from_market_context",
    "project_economic_calendar_state",
    "query_economic_calendar_as_known",
    "read_economic_calendar_corpus",
    "replay_economic_calendar_corpus",
    "write_economic_calendar_corpus",
]
