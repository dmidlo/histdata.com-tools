"""Versioned point-in-time market-context contracts and bounded queries.

This module keeps macro, central-bank, news, and calendar context outside the
row-aligned analytical surface.  Context is stored once as an immutable,
provenance-rich timeline and joined to bounded reconstruction windows through
small query sidecars.  Version-one contracts never fetch or license a source;
adapters normalize operator-approved evidence into these contracts.
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
from typing import Any, Protocol, TypeVar, cast, runtime_checkable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from histdatacom.data_quality.calendar import (
    calendar_policy_metadata,
    classify_histdata_timestamp,
)
from histdatacom.data_quality.calendar_profiles import HistDataCalendarProfile
from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.information import (
    InformationInputKind,
    InformationMode,
    InformationScope,
    InformationSplitKind,
    InformationStage,
    ReconstructionInformationInputV1,
)
from histdatacom.synthetic.streaming import ReconstructionWindowV1

MARKET_CONTEXT_SOURCE_SCHEMA_VERSION = "histdatacom.market-context-source.v1"
MARKET_CONTEXT_EVENT_SCHEMA_VERSION = "histdatacom.market-context-event.v1"
MARKET_CONTEXT_TIMELINE_SCHEMA_VERSION = (
    "histdatacom.market-context-timeline.v1"
)
MARKET_CONTEXT_CALENDAR_STATE_SCHEMA_VERSION = (
    "histdatacom.market-context-calendar-state.v1"
)
MARKET_CONTEXT_QUERY_SCHEMA_VERSION = "histdatacom.market-context-query.v1"

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
MAX_MARKET_CONTEXT_EVENTS = 4096
MAX_MARKET_CONTEXT_QUERY_EVENTS = 512
MAX_MARKET_CONTEXT_ADAPTERS = 64
MAX_MARKET_CONTEXT_ITEMS = 64
MAX_MARKET_CONTEXT_TEXT = 1024
MAX_MARKET_CONTEXT_METADATA_BYTES = 16_384
MAX_MARKET_CONTEXT_TIMELINE_BYTES = 16_777_216

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CANONICAL_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]*$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9._:-]{3,32}$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")
_EnumT = TypeVar("_EnumT", bound=Enum)


class MarketContextKind(str, Enum):
    """Stable semantic kinds for external market-context records."""

    MACRO_RELEASE = "macro_release"
    CENTRAL_BANK_DECISION = "central_bank_decision"
    CENTRAL_BANK_STATEMENT = "central_bank_statement"
    SCHEDULED_COMMUNICATION = "scheduled_communication"
    UNSCHEDULED_SHOCK = "unscheduled_shock"
    NEWS_WINDOW = "news_window"

    @classmethod
    def from_value(
        cls, value: str | "MarketContextKind"
    ) -> "MarketContextKind":
        """Return a strict normalized context kind."""
        return _enum_value(cls, value, "market context kind")


class MarketContextPrecision(str, Enum):
    """How precisely the event time and semantic boundary are known."""

    EXACT = "exact"
    APPROXIMATE = "approximate"
    WINDOW_ONLY = "window_only"

    @classmethod
    def from_value(
        cls, value: str | "MarketContextPrecision"
    ) -> "MarketContextPrecision":
        """Return a strict normalized precision value."""
        return _enum_value(cls, value, "market context precision")


class MarketContextView(str, Enum):
    """Whether a query exposes all vintages or only point-in-time knowledge."""

    EX_POST = "ex_post"
    EX_ANTE = "ex_ante"

    @classmethod
    def from_value(
        cls, value: str | "MarketContextView"
    ) -> "MarketContextView":
        """Return a strict normalized query view."""
        return _enum_value(cls, value, "market context view")


class MarketContextQueryStatus(str, Enum):
    """Whether an event query matched explicit context."""

    MATCHED = "matched"
    MISSING = "missing"

    @classmethod
    def from_value(
        cls, value: str | "MarketContextQueryStatus"
    ) -> "MarketContextQueryStatus":
        """Return a strict normalized query status."""
        return _enum_value(cls, value, "market context query status")


class MarketContextMissingReason(str, Enum):
    """Stable explanations for an empty context query."""

    NO_MATCHING_EVENT = "no_matching_event"
    NOT_AVAILABLE_AS_OF = "not_available_as_of"
    OUTSIDE_TIMELINE_COVERAGE = "outside_timeline_coverage"
    TIMELINE_INCOMPLETE = "timeline_incomplete"

    @classmethod
    def from_value(
        cls, value: str | "MarketContextMissingReason"
    ) -> "MarketContextMissingReason":
        """Return a strict normalized missing-context reason."""
        return _enum_value(cls, value, "market context missing reason")


class MarketContextQueryLimitError(ValueError):
    """Raised when a bounded context query would retain too many events."""


@dataclass(frozen=True, slots=True)
class MarketContextSourceV1:
    """Immutable provenance and redistribution policy for one source object."""

    name: str
    source_version: str
    retrieved_at_ns: int
    content_sha256: str
    adapter_name: str
    adapter_version: str
    license_name: str
    redistribution_allowed: bool
    redistribution_constraints: tuple[str, ...]
    limitations: tuple[str, ...]
    source_uri: str | None = None
    metadata: Mapping[str, JSONValue] | None = None
    source_id: str = ""
    schema_version: str = MARKET_CONTEXT_SOURCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_CONTEXT_SOURCE_SCHEMA_VERSION:
            raise ValueError("unsupported market context source schema")
        for name in (
            "name",
            "source_version",
            "adapter_name",
            "adapter_version",
            "license_name",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "retrieved_at_ns",
            _bounded_int64(self.retrieved_at_ns, "retrieved_at_ns"),
        )
        object.__setattr__(
            self,
            "content_sha256",
            _required_sha256(self.content_sha256, "content_sha256"),
        )
        if not isinstance(self.redistribution_allowed, bool):
            raise ValueError("redistribution_allowed must be a boolean")
        constraints = _bounded_text_tuple(
            self.redistribution_constraints,
            "redistribution_constraints",
        )
        if not self.redistribution_allowed and not constraints:
            raise ValueError(
                "non-redistributable sources require explicit constraints"
            )
        limitations = _bounded_text_tuple(self.limitations, "limitations")
        if not limitations:
            raise ValueError("market context sources require limitations")
        object.__setattr__(self, "redistribution_constraints", constraints)
        object.__setattr__(self, "limitations", limitations)
        object.__setattr__(self, "source_uri", _optional_text(self.source_uri))
        metadata = dict(self.metadata or {})
        _validate_json_value(metadata, "source.metadata")
        _ensure_payload_size(metadata, MAX_MARKET_CONTEXT_METADATA_BYTES)
        object.__setattr__(self, "metadata", metadata)
        expected = _stable_id("market-context-source", self.identity_payload())
        supplied = _optional_text(self.source_id)
        if supplied is not None and supplied != expected:
            raise ValueError("source_id does not match deterministic identity")
        object.__setattr__(self, "source_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic source identity payload."""
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "source_version": self.source_version,
            "retrieved_at_ns": self.retrieved_at_ns,
            "content_sha256": self.content_sha256,
            "adapter_name": self.adapter_name,
            "adapter_version": self.adapter_version,
            "license_name": self.license_name,
            "redistribution_allowed": self.redistribution_allowed,
            "redistribution_constraints": list(self.redistribution_constraints),
            "limitations": list(self.limitations),
            "source_uri": self.source_uri,
            "metadata": dict(self.metadata or {}),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible source metadata."""
        return {**self.identity_payload(), "source_id": self.source_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MarketContextSourceV1":
        """Restore and verify one source contract."""
        _require_schema(data, MARKET_CONTEXT_SOURCE_SCHEMA_VERSION)
        return cls(
            name=str(data.get("name", "")),
            source_version=str(data.get("source_version", "")),
            retrieved_at_ns=cast(int, data.get("retrieved_at_ns")),
            content_sha256=str(data.get("content_sha256", "")),
            adapter_name=str(data.get("adapter_name", "")),
            adapter_version=str(data.get("adapter_version", "")),
            license_name=str(data.get("license_name", "")),
            redistribution_allowed=_strict_bool(
                data.get("redistribution_allowed"),
                "redistribution_allowed",
            ),
            redistribution_constraints=_string_tuple(
                data.get("redistribution_constraints")
            ),
            limitations=_string_tuple(data.get("limitations")),
            source_uri=_optional_text(data.get("source_uri")),
            metadata=_mapping(data.get("metadata")),
            source_id=str(data.get("source_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "MarketContextSourceV1":
        """Restore a source from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class MarketContextEventV1:
    """One immutable event vintage with explicit knowledge-time semantics."""

    canonical_key: str
    kind: MarketContextKind
    title: str
    source: MarketContextSourceV1
    source_event_time: str
    source_timezone: str
    event_time_ns: int
    first_known_at_ns: int
    available_at_ns: int
    pre_event_ns: int
    post_event_ns: int
    affected_currencies: tuple[str, ...]
    affected_symbols: tuple[str, ...]
    confidence: float
    precision: MarketContextPrecision
    limitations: tuple[str, ...]
    vintage_id: str
    revision_sequence: int = 0
    supersedes_event_id: str | None = None
    source_time_fold: int | None = None
    ambiguity_reason: str | None = None
    expected_value: float | None = None
    actual_value: float | None = None
    previous_value: float | None = None
    revised_previous_value: float | None = None
    surprise_value: float | None = None
    value_unit: str | None = None
    content_sha256: str | None = None
    tags: tuple[str, ...] = ()
    event_id: str = ""
    schema_version: str = MARKET_CONTEXT_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_CONTEXT_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported market context event schema")
        if not isinstance(self.source, MarketContextSourceV1):
            raise ValueError("source must be a market context source")
        object.__setattr__(
            self,
            "canonical_key",
            _canonical_key(self.canonical_key),
        )
        object.__setattr__(
            self, "kind", MarketContextKind.from_value(self.kind)
        )
        object.__setattr__(self, "title", _required_text(self.title))
        object.__setattr__(
            self, "source_event_time", _required_text(self.source_event_time)
        )
        object.__setattr__(
            self, "source_timezone", _required_text(self.source_timezone)
        )
        fold = _optional_fold(self.source_time_fold)
        object.__setattr__(self, "source_time_fold", fold)
        event_time = _bounded_int64(self.event_time_ns, "event_time_ns")
        normalized_time = normalize_market_context_datetime(
            self.source_event_time,
            self.source_timezone,
            fold=fold,
        )
        if normalized_time != event_time:
            raise ValueError(
                "event_time_ns does not match normalized source event time"
            )
        object.__setattr__(self, "event_time_ns", event_time)
        first_known = _bounded_int64(
            self.first_known_at_ns, "first_known_at_ns"
        )
        available = _bounded_int64(self.available_at_ns, "available_at_ns")
        if first_known > available:
            raise ValueError(
                "first_known_at_ns must not follow available_at_ns"
            )
        if self.source.retrieved_at_ns < available:
            raise ValueError(
                "source retrieval cannot precede record availability"
            )
        object.__setattr__(self, "first_known_at_ns", first_known)
        object.__setattr__(self, "available_at_ns", available)
        pre = _nonnegative_int64(self.pre_event_ns, "pre_event_ns")
        post = _positive_int64(self.post_event_ns, "post_event_ns")
        _bounded_int64(event_time - pre, "window_start_ns")
        _bounded_int64(event_time + post, "window_end_ns")
        object.__setattr__(self, "pre_event_ns", pre)
        object.__setattr__(self, "post_event_ns", post)
        currencies = _normalized_currencies(self.affected_currencies)
        symbols = _normalized_symbols(self.affected_symbols)
        if not currencies and not symbols:
            raise ValueError(
                "market context events require an affected currency or symbol"
            )
        object.__setattr__(self, "affected_currencies", currencies)
        object.__setattr__(self, "affected_symbols", symbols)
        confidence = _finite_float(self.confidence, "confidence")
        if not 0.0 <= confidence <= 1.0:
            raise ValueError("confidence must be between zero and one")
        object.__setattr__(self, "confidence", confidence)
        precision = MarketContextPrecision.from_value(self.precision)
        object.__setattr__(self, "precision", precision)
        ambiguity = _optional_text(self.ambiguity_reason)
        if self.kind in {
            MarketContextKind.UNSCHEDULED_SHOCK,
            MarketContextKind.NEWS_WINDOW,
        }:
            if precision is MarketContextPrecision.EXACT or ambiguity is None:
                raise ValueError(
                    "unscheduled/news context requires non-exact precision "
                    "and an ambiguity reason"
                )
        object.__setattr__(self, "ambiguity_reason", ambiguity)
        limitations = _bounded_text_tuple(self.limitations, "limitations")
        if not limitations:
            raise ValueError("market context events require limitations")
        object.__setattr__(self, "limitations", limitations)
        object.__setattr__(self, "vintage_id", _required_text(self.vintage_id))
        revision = _nonnegative_int(self.revision_sequence, "revision_sequence")
        supersedes = _optional_text(self.supersedes_event_id)
        if revision == 0 and supersedes is not None:
            raise ValueError("an initial event cannot supersede another event")
        if revision > 0 and supersedes is None:
            raise ValueError("a revised event requires supersedes_event_id")
        object.__setattr__(self, "revision_sequence", revision)
        object.__setattr__(self, "supersedes_event_id", supersedes)
        for name in (
            "expected_value",
            "actual_value",
            "previous_value",
            "revised_previous_value",
            "surprise_value",
        ):
            object.__setattr__(
                self, name, _optional_finite(getattr(self, name), name)
            )
        if self.expected_value is not None and self.actual_value is not None:
            expected_surprise = self.actual_value - self.expected_value
            if self.surprise_value is None:
                object.__setattr__(self, "surprise_value", expected_surprise)
            elif not math.isclose(
                self.surprise_value,
                expected_surprise,
                rel_tol=1e-12,
                abs_tol=1e-12,
            ):
                raise ValueError(
                    "surprise_value must equal actual minus expected"
                )
        elif self.surprise_value is not None:
            raise ValueError(
                "surprise_value requires expected_value and actual_value"
            )
        object.__setattr__(self, "value_unit", _optional_text(self.value_unit))
        content_hash = _optional_text(self.content_sha256)
        if content_hash is not None:
            content_hash = _required_sha256(content_hash, "content_sha256")
        object.__setattr__(self, "content_sha256", content_hash)
        object.__setattr__(
            self,
            "tags",
            tuple(sorted(_bounded_text_tuple(self.tags, "tags"))),
        )
        expected = _stable_id("market-context-event", self.identity_payload())
        supplied = _optional_text(self.event_id)
        if supplied is not None and supplied != expected:
            raise ValueError("event_id does not match deterministic identity")
        object.__setattr__(self, "event_id", expected)

    @property
    def window_start_ns(self) -> int:
        """Return the inclusive conditioned-window start."""
        return self.event_time_ns - self.pre_event_ns

    @property
    def window_end_ns(self) -> int:
        """Return the exclusive conditioned-window end."""
        return self.event_time_ns + self.post_event_ns

    def overlaps(self, start_ns: int, end_ns: int) -> bool:
        """Return whether this event's conditioned window overlaps a query."""
        start = _bounded_int64(start_ns, "start_ns")
        end = _bounded_int64(end_ns, "end_ns")
        if end <= start:
            raise ValueError("query end_ns must be greater than start_ns")
        return self.window_start_ns < end and self.window_end_ns > start

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic event identity payload."""
        return {
            "schema_version": self.schema_version,
            "canonical_key": self.canonical_key,
            "kind": self.kind.value,
            "title": self.title,
            "source": self.source.to_dict(),
            "source_event_time": self.source_event_time,
            "source_timezone": self.source_timezone,
            "source_time_fold": self.source_time_fold,
            "event_time_ns": self.event_time_ns,
            "first_known_at_ns": self.first_known_at_ns,
            "available_at_ns": self.available_at_ns,
            "pre_event_ns": self.pre_event_ns,
            "post_event_ns": self.post_event_ns,
            "window_start_ns": self.window_start_ns,
            "window_end_ns": self.window_end_ns,
            "window_semantics": "[event-pre,event+post)",
            "affected_currencies": list(self.affected_currencies),
            "affected_symbols": list(self.affected_symbols),
            "confidence": self.confidence,
            "precision": self.precision.value,
            "ambiguity_reason": self.ambiguity_reason,
            "limitations": list(self.limitations),
            "vintage_id": self.vintage_id,
            "revision_sequence": self.revision_sequence,
            "supersedes_event_id": self.supersedes_event_id,
            "expected_value": self.expected_value,
            "actual_value": self.actual_value,
            "previous_value": self.previous_value,
            "revised_previous_value": self.revised_previous_value,
            "surprise_value": self.surprise_value,
            "value_unit": self.value_unit,
            "content_sha256": self.content_sha256,
            "tags": list(self.tags),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible event metadata."""
        return {**self.identity_payload(), "event_id": self.event_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MarketContextEventV1":
        """Restore and verify one market-context event."""
        _require_schema(data, MARKET_CONTEXT_EVENT_SCHEMA_VERSION)
        return cls(
            canonical_key=str(data.get("canonical_key", "")),
            kind=MarketContextKind.from_value(str(data.get("kind", ""))),
            title=str(data.get("title", "")),
            source=MarketContextSourceV1.from_dict(
                _mapping(data.get("source"))
            ),
            source_event_time=str(data.get("source_event_time", "")),
            source_timezone=str(data.get("source_timezone", "")),
            source_time_fold=_optional_int(data.get("source_time_fold")),
            event_time_ns=cast(int, data.get("event_time_ns")),
            first_known_at_ns=cast(int, data.get("first_known_at_ns")),
            available_at_ns=cast(int, data.get("available_at_ns")),
            pre_event_ns=cast(int, data.get("pre_event_ns", 0)),
            post_event_ns=cast(int, data.get("post_event_ns", 1)),
            affected_currencies=_string_tuple(data.get("affected_currencies")),
            affected_symbols=_string_tuple(data.get("affected_symbols")),
            confidence=cast(float, data.get("confidence")),
            precision=MarketContextPrecision.from_value(
                str(data.get("precision", ""))
            ),
            ambiguity_reason=_optional_text(data.get("ambiguity_reason")),
            limitations=_string_tuple(data.get("limitations")),
            vintage_id=str(data.get("vintage_id", "")),
            revision_sequence=cast(int, data.get("revision_sequence", 0)),
            supersedes_event_id=_optional_text(data.get("supersedes_event_id")),
            expected_value=_optional_number(data.get("expected_value")),
            actual_value=_optional_number(data.get("actual_value")),
            previous_value=_optional_number(data.get("previous_value")),
            revised_previous_value=_optional_number(
                data.get("revised_previous_value")
            ),
            surprise_value=_optional_number(data.get("surprise_value")),
            value_unit=_optional_text(data.get("value_unit")),
            content_sha256=_optional_text(data.get("content_sha256")),
            tags=_string_tuple(data.get("tags")),
            event_id=str(data.get("event_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "MarketContextEventV1":
        """Restore an event from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class MarketContextTimelineV1:
    """Immutable ordered context vintages over one declared coverage range."""

    timeline_version: str
    coverage_start_ns: int
    coverage_end_ns: int
    complete: bool
    events: tuple[MarketContextEventV1, ...]
    limitations: tuple[str, ...]
    timeline_id: str = ""
    schema_version: str = MARKET_CONTEXT_TIMELINE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_CONTEXT_TIMELINE_SCHEMA_VERSION:
            raise ValueError("unsupported market context timeline schema")
        object.__setattr__(
            self, "timeline_version", _required_text(self.timeline_version)
        )
        start = _bounded_int64(self.coverage_start_ns, "coverage_start_ns")
        end = _bounded_int64(self.coverage_end_ns, "coverage_end_ns")
        if end <= start:
            raise ValueError(
                "coverage_end_ns must be greater than coverage_start_ns"
            )
        if not isinstance(self.complete, bool):
            raise ValueError("complete must be a boolean")
        values = tuple(self.events)
        if len(values) > MAX_MARKET_CONTEXT_EVENTS:
            raise ValueError("market context timeline exceeds event limit")
        if any(not isinstance(item, MarketContextEventV1) for item in values):
            raise ValueError("events must contain market context events")
        ordered = tuple(
            sorted(
                values,
                key=lambda item: (
                    item.event_time_ns,
                    item.canonical_key,
                    item.revision_sequence,
                    item.event_id,
                ),
            )
        )
        _validate_timeline_events(ordered, start, end)
        limitations = _bounded_text_tuple(self.limitations, "limitations")
        if not limitations:
            raise ValueError("market context timelines require limitations")
        object.__setattr__(self, "coverage_start_ns", start)
        object.__setattr__(self, "coverage_end_ns", end)
        object.__setattr__(self, "events", ordered)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "market-context-timeline", self.identity_payload()
        )
        supplied = _optional_text(self.timeline_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "timeline_id does not match deterministic identity"
            )
        object.__setattr__(self, "timeline_id", expected)
        _ensure_payload_size(self.to_dict(), MAX_MARKET_CONTEXT_TIMELINE_BYTES)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic timeline identity payload."""
        return {
            "schema_version": self.schema_version,
            "timeline_version": self.timeline_version,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "coverage_semantics": "[coverage_start_ns,coverage_end_ns)",
            "complete": self.complete,
            "events": [item.to_dict() for item in self.events],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible timeline metadata."""
        return {**self.identity_payload(), "timeline_id": self.timeline_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MarketContextTimelineV1":
        """Restore and verify one immutable timeline."""
        _require_schema(data, MARKET_CONTEXT_TIMELINE_SCHEMA_VERSION)
        return cls(
            timeline_version=str(data.get("timeline_version", "")),
            coverage_start_ns=cast(int, data.get("coverage_start_ns")),
            coverage_end_ns=cast(int, data.get("coverage_end_ns")),
            complete=_strict_bool(data.get("complete"), "complete"),
            events=tuple(
                MarketContextEventV1.from_dict(item)
                for item in _mapping_sequence(data.get("events"))
            ),
            limitations=_string_tuple(data.get("limitations")),
            timeline_id=str(data.get("timeline_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "MarketContextTimelineV1":
        """Restore a timeline from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class MarketContextCalendarStateV1:
    """Compact calendar/session classification for one window timestamp."""

    timestamp_utc_ns: int
    session_state: str
    clock_sessions: tuple[str, ...]
    active_sessions: tuple[str, ...]
    overlaps: tuple[str, ...]
    special_tags: tuple[str, ...]
    holiday_tags: tuple[str, ...]
    event_tags: tuple[str, ...]
    calendar_tags: tuple[str, ...]
    profile_source: str
    profile_version: str
    profile_complete: bool
    limitations: tuple[str, ...]
    state_id: str = ""
    schema_version: str = MARKET_CONTEXT_CALENDAR_STATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_CONTEXT_CALENDAR_STATE_SCHEMA_VERSION:
            raise ValueError("unsupported market context calendar-state schema")
        object.__setattr__(
            self,
            "timestamp_utc_ns",
            _bounded_int64(self.timestamp_utc_ns, "timestamp_utc_ns"),
        )
        for name in ("session_state", "profile_source", "profile_version"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(self.profile_complete, bool):
            raise ValueError("profile_complete must be a boolean")
        for name in (
            "clock_sessions",
            "active_sessions",
            "overlaps",
            "special_tags",
            "holiday_tags",
            "event_tags",
            "calendar_tags",
            "limitations",
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text_tuple(getattr(self, name), name),
            )
        if not self.limitations:
            raise ValueError("calendar states require explicit limitations")
        expected = _stable_id(
            "market-context-calendar", self.identity_payload()
        )
        supplied = _optional_text(self.state_id)
        if supplied is not None and supplied != expected:
            raise ValueError("state_id does not match deterministic identity")
        object.__setattr__(self, "state_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic calendar-state payload."""
        return {
            "schema_version": self.schema_version,
            "timestamp_utc_ns": self.timestamp_utc_ns,
            "session_state": self.session_state,
            "clock_sessions": list(self.clock_sessions),
            "active_sessions": list(self.active_sessions),
            "overlaps": list(self.overlaps),
            "special_tags": list(self.special_tags),
            "holiday_tags": list(self.holiday_tags),
            "event_tags": list(self.event_tags),
            "calendar_tags": list(self.calendar_tags),
            "profile_source": self.profile_source,
            "profile_version": self.profile_version,
            "profile_complete": self.profile_complete,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible calendar metadata."""
        return {**self.identity_payload(), "state_id": self.state_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "MarketContextCalendarStateV1":
        """Restore and verify one calendar state."""
        _require_schema(data, MARKET_CONTEXT_CALENDAR_STATE_SCHEMA_VERSION)
        return cls(
            timestamp_utc_ns=cast(int, data.get("timestamp_utc_ns")),
            session_state=str(data.get("session_state", "")),
            clock_sessions=_string_tuple(data.get("clock_sessions")),
            active_sessions=_string_tuple(data.get("active_sessions")),
            overlaps=_string_tuple(data.get("overlaps")),
            special_tags=_string_tuple(data.get("special_tags")),
            holiday_tags=_string_tuple(data.get("holiday_tags")),
            event_tags=_string_tuple(data.get("event_tags")),
            calendar_tags=_string_tuple(data.get("calendar_tags")),
            profile_source=str(data.get("profile_source", "")),
            profile_version=str(data.get("profile_version", "")),
            profile_complete=_strict_bool(
                data.get("profile_complete"), "profile_complete"
            ),
            limitations=_string_tuple(data.get("limitations")),
            state_id=str(data.get("state_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "MarketContextCalendarStateV1":
        """Restore a calendar state from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class MarketContextQueryV1:
    """Bounded event/calendar sidecar for one reconstruction window."""

    timeline_id: str
    view: MarketContextView
    start_ns: int
    end_ns: int
    as_of_ns: int | None
    events: tuple[MarketContextEventV1, ...]
    status: MarketContextQueryStatus
    missing_reason: MarketContextMissingReason | None
    calendar_state: MarketContextCalendarStateV1 | None
    requested_currencies: tuple[str, ...] = ()
    requested_symbols: tuple[str, ...] = ()
    requested_kinds: tuple[MarketContextKind, ...] = ()
    window_id: str | None = None
    limitations: tuple[str, ...] = ()
    query_id: str = ""
    schema_version: str = MARKET_CONTEXT_QUERY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_CONTEXT_QUERY_SCHEMA_VERSION:
            raise ValueError("unsupported market context query schema")
        object.__setattr__(
            self, "timeline_id", _required_text(self.timeline_id)
        )
        object.__setattr__(
            self, "view", MarketContextView.from_value(self.view)
        )
        start = _bounded_int64(self.start_ns, "start_ns")
        end = _bounded_int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("query end_ns must be greater than start_ns")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        as_of = (
            None
            if self.as_of_ns is None
            else _bounded_int64(self.as_of_ns, "as_of_ns")
        )
        if self.view is MarketContextView.EX_ANTE and as_of is None:
            raise ValueError("ex-ante context queries require as_of_ns")
        object.__setattr__(self, "as_of_ns", as_of)
        events = tuple(self.events)
        if len(events) > MAX_MARKET_CONTEXT_QUERY_EVENTS:
            raise ValueError("market context query exceeds event limit")
        if any(not isinstance(item, MarketContextEventV1) for item in events):
            raise ValueError("query events must contain market context events")
        object.__setattr__(self, "events", events)
        status = MarketContextQueryStatus.from_value(self.status)
        reason = self.missing_reason
        if reason is not None:
            reason = MarketContextMissingReason.from_value(reason)
        if events and (
            status is not MarketContextQueryStatus.MATCHED or reason
        ):
            raise ValueError("matched context cannot carry a missing reason")
        if not events and (
            status is not MarketContextQueryStatus.MISSING or reason is None
        ):
            raise ValueError("empty context requires a missing reason")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "missing_reason", reason)
        if self.calendar_state is not None and not isinstance(
            self.calendar_state, MarketContextCalendarStateV1
        ):
            raise ValueError("calendar_state must use the v1 contract")
        object.__setattr__(
            self,
            "requested_currencies",
            _normalized_currencies(self.requested_currencies),
        )
        object.__setattr__(
            self,
            "requested_symbols",
            _normalized_symbols(self.requested_symbols),
        )
        kinds = tuple(
            sorted(
                {
                    MarketContextKind.from_value(item)
                    for item in self.requested_kinds
                },
                key=lambda item: item.value,
            )
        )
        object.__setattr__(self, "requested_kinds", kinds)
        object.__setattr__(self, "window_id", _optional_text(self.window_id))
        object.__setattr__(
            self,
            "limitations",
            _bounded_text_tuple(self.limitations, "limitations"),
        )
        expected = _stable_id("market-context-query", self.identity_payload())
        supplied = _optional_text(self.query_id)
        if supplied is not None and supplied != expected:
            raise ValueError("query_id does not match deterministic identity")
        object.__setattr__(self, "query_id", expected)

    @property
    def information_mode(self) -> InformationMode:
        """Return the reconstruction information mode implied by this view."""
        if self.view is MarketContextView.EX_ANTE:
            return InformationMode.EX_ANTE_SIMULATION
        return InformationMode.EX_POST_RECONSTRUCTION

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic query identity payload."""
        return {
            "schema_version": self.schema_version,
            "timeline_id": self.timeline_id,
            "view": self.view.value,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "interval_semantics": "[start_ns,end_ns)",
            "as_of_ns": self.as_of_ns,
            "events": [item.to_dict() for item in self.events],
            "status": self.status.value,
            "missing_reason": (
                self.missing_reason.value if self.missing_reason else None
            ),
            "calendar_state": (
                self.calendar_state.to_dict() if self.calendar_state else None
            ),
            "requested_currencies": list(self.requested_currencies),
            "requested_symbols": list(self.requested_symbols),
            "requested_kinds": [item.value for item in self.requested_kinds],
            "window_id": self.window_id,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible query metadata."""
        return {**self.identity_payload(), "query_id": self.query_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MarketContextQueryV1":
        """Restore and verify one query sidecar."""
        _require_schema(data, MARKET_CONTEXT_QUERY_SCHEMA_VERSION)
        reason = data.get("missing_reason")
        calendar = data.get("calendar_state")
        return cls(
            timeline_id=str(data.get("timeline_id", "")),
            view=MarketContextView.from_value(str(data.get("view", ""))),
            start_ns=cast(int, data.get("start_ns")),
            end_ns=cast(int, data.get("end_ns")),
            as_of_ns=_optional_int(data.get("as_of_ns")),
            events=tuple(
                MarketContextEventV1.from_dict(item)
                for item in _mapping_sequence(data.get("events"))
            ),
            status=MarketContextQueryStatus.from_value(
                str(data.get("status", ""))
            ),
            missing_reason=(
                MarketContextMissingReason.from_value(str(reason))
                if reason is not None
                else None
            ),
            calendar_state=(
                MarketContextCalendarStateV1.from_dict(_mapping(calendar))
                if calendar is not None
                else None
            ),
            requested_currencies=_string_tuple(
                data.get("requested_currencies")
            ),
            requested_symbols=_string_tuple(data.get("requested_symbols")),
            requested_kinds=tuple(
                MarketContextKind.from_value(str(item))
                for item in _sequence(data.get("requested_kinds"))
            ),
            window_id=_optional_text(data.get("window_id")),
            limitations=_string_tuple(data.get("limitations")),
            query_id=str(data.get("query_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "MarketContextQueryV1":
        """Restore a query from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@runtime_checkable
class MarketContextSourceAdapterV1(Protocol):
    """Shared adapter seam for operator-approved context sources."""

    @property
    def adapter_name(self) -> str:
        """Return the immutable adapter family name."""

    @property
    def adapter_version(self) -> str:
        """Return the immutable adapter implementation version."""

    def load_events(self) -> Iterable[MarketContextEventV1]:
        """Yield normalized immutable event vintages."""


@dataclass(frozen=True, slots=True)
class StaticMarketContextSourceAdapterV1:
    """Deterministic in-memory adapter for fixtures and normalized imports."""

    adapter_name: str
    adapter_version: str
    events: tuple[MarketContextEventV1, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "adapter_name", _required_text(self.adapter_name)
        )
        object.__setattr__(
            self, "adapter_version", _required_text(self.adapter_version)
        )
        events = tuple(self.events)
        if len(events) > MAX_MARKET_CONTEXT_EVENTS:
            raise ValueError("static adapter exceeds event limit")
        for event in events:
            if not isinstance(event, MarketContextEventV1):
                raise ValueError("static adapter events must use v1 contracts")
            _verify_adapter_event(self, event)
        object.__setattr__(self, "events", events)

    def load_events(self) -> Iterable[MarketContextEventV1]:
        """Yield the immutable configured event vintages."""
        return iter(self.events)


def build_market_context_timeline(
    adapters: Sequence[MarketContextSourceAdapterV1],
    *,
    timeline_version: str,
    coverage_start_ns: int,
    coverage_end_ns: int,
    complete: bool,
    limitations: Sequence[str],
) -> MarketContextTimelineV1:
    """Collect bounded adapter output into one deterministic timeline."""
    values = tuple(adapters)
    if len(values) > MAX_MARKET_CONTEXT_ADAPTERS:
        raise ValueError("market context adapter count exceeds v1 limit")
    events: list[MarketContextEventV1] = []
    for adapter in values:
        if not isinstance(adapter, MarketContextSourceAdapterV1):
            raise ValueError("adapter does not implement the v1 context seam")
        _required_text(adapter.adapter_name)
        _required_text(adapter.adapter_version)
        for event in adapter.load_events():
            if not isinstance(event, MarketContextEventV1):
                raise ValueError("adapter emitted a non-v1 context event")
            _verify_adapter_event(adapter, event)
            events.append(event)
            if len(events) > MAX_MARKET_CONTEXT_EVENTS:
                raise ValueError("market context adapters exceeded event limit")
    return MarketContextTimelineV1(
        timeline_version=timeline_version,
        coverage_start_ns=coverage_start_ns,
        coverage_end_ns=coverage_end_ns,
        complete=complete,
        events=tuple(events),
        limitations=tuple(limitations),
    )


def normalize_market_context_datetime(
    value: str,
    timezone_name: str,
    *,
    fold: int | None = None,
) -> int:
    """Normalize one ISO source time to exact UTC nanoseconds.

    Naive timestamps require an IANA timezone.  Ambiguous daylight-saving
    folds require an explicit ``fold`` value and nonexistent wall times fail
    closed.  Offset-aware source strings are checked against the named zone.
    """
    text = _required_text(value)
    zone_name = _required_text(timezone_name)
    try:
        zone = ZoneInfo(zone_name)
    except ZoneInfoNotFoundError as err:
        raise ValueError("unsupported source timezone") from err
    try:
        parsed = datetime.fromisoformat(
            text.removesuffix("Z") + ("+00:00" if text.endswith("Z") else "")
        )
    except ValueError as err:
        raise ValueError("source_event_time must be ISO-8601") from err
    fold_value = _optional_fold(fold)
    if parsed.tzinfo is not None:
        if fold_value is not None:
            raise ValueError("source_time_fold is only valid for naive times")
        projected = parsed.astimezone(zone)
        if (
            projected.replace(tzinfo=None) != parsed.replace(tzinfo=None)
            or projected.utcoffset() != parsed.utcoffset()
        ):
            raise ValueError("source offset does not match source timezone")
        aware = parsed
    else:
        candidates: dict[int, datetime] = {}
        for candidate_fold in (0, 1):
            candidate = parsed.replace(tzinfo=zone, fold=candidate_fold)
            round_trip = (
                candidate.astimezone(timezone.utc)
                .astimezone(zone)
                .replace(tzinfo=None)
            )
            if round_trip == parsed:
                offset = candidate.utcoffset()
                if offset is not None:
                    candidates[int(offset.total_seconds())] = candidate
        if not candidates:
            raise ValueError("source event time is nonexistent in its timezone")
        if len(candidates) > 1 and fold_value is None:
            raise ValueError(
                "ambiguous source event time requires source_time_fold"
            )
        if fold_value is None:
            aware = next(iter(candidates.values()))
        else:
            aware = parsed.replace(tzinfo=zone, fold=fold_value)
            round_trip = (
                aware.astimezone(timezone.utc)
                .astimezone(zone)
                .replace(tzinfo=None)
            )
            if round_trip != parsed:
                raise ValueError(
                    "source_time_fold does not resolve the wall time"
                )
    utc = aware.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = utc - epoch
    value_ns = (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )
    return _bounded_int64(value_ns, "normalized event time")


def market_context_calendar_state(
    timestamp_utc_ns: int,
    *,
    calendar_profile: HistDataCalendarProfile | None = None,
    asset_class: str = "fx",
) -> MarketContextCalendarStateV1:
    """Reuse the canonical HistData calendar classifier for one UTC instant."""
    timestamp_ns = _bounded_int64(timestamp_utc_ns, "timestamp_utc_ns")
    classification = classify_histdata_timestamp(
        timestamp_ns // 1_000_000,
        calendar_profile=calendar_profile,
        asset_class=asset_class,
    )
    policy = calendar_policy_metadata(calendar_profile)
    profile = _mapping(policy.get("calendar_profile"))
    limitation = str(policy.get("holiday_calendar_limitations") or "").strip()
    limitations = (
        limitation
        or "Calendar state is a deterministic classifier, not an event corpus.",
    )
    return MarketContextCalendarStateV1(
        timestamp_utc_ns=timestamp_ns,
        session_state=classification.session_state,
        clock_sessions=classification.clock_sessions,
        active_sessions=classification.active_sessions,
        overlaps=classification.overlaps,
        special_tags=classification.special_tags,
        holiday_tags=classification.holiday_tags,
        event_tags=classification.event_tags,
        calendar_tags=classification.calendar_tags,
        profile_source=str(policy.get("holiday_calendar_source") or "unknown"),
        profile_version=str(profile.get("version") or "unversioned"),
        profile_complete=bool(policy.get("holiday_calendar_complete")),
        limitations=limitations,
    )


def query_market_context(
    timeline: MarketContextTimelineV1,
    *,
    start_ns: int,
    end_ns: int,
    view: MarketContextView,
    as_of_ns: int | None = None,
    currencies: Sequence[str] = (),
    symbols: Sequence[str] = (),
    kinds: Sequence[MarketContextKind] = (),
    include_calendar: bool = True,
    calendar_at_ns: int | None = None,
    calendar_profile: HistDataCalendarProfile | None = None,
    max_events: int = MAX_MARKET_CONTEXT_QUERY_EVENTS,
    window_id: str | None = None,
) -> MarketContextQueryV1:
    """Return a bounded ex-post or point-in-time-safe context sidecar."""
    if not isinstance(timeline, MarketContextTimelineV1):
        raise ValueError("timeline must use the v1 market-context contract")
    start = _bounded_int64(start_ns, "start_ns")
    end = _bounded_int64(end_ns, "end_ns")
    if end <= start:
        raise ValueError("query end_ns must be greater than start_ns")
    selected_view = MarketContextView.from_value(view)
    as_of = None if as_of_ns is None else _bounded_int64(as_of_ns, "as_of_ns")
    if selected_view is MarketContextView.EX_ANTE and as_of is None:
        raise ValueError("ex-ante context queries require as_of_ns")
    requested_currencies = _normalized_currencies(currencies)
    requested_symbols = _normalized_symbols(symbols)
    requested_kinds = tuple(
        sorted(
            {MarketContextKind.from_value(item) for item in kinds},
            key=lambda item: item.value,
        )
    )
    limit = _positive_int(max_events, "max_events")
    if limit > MAX_MARKET_CONTEXT_QUERY_EVENTS:
        raise ValueError("max_events exceeds the v1 query limit")
    matched: list[MarketContextEventV1] = []
    hidden_by_availability = False
    for event in timeline.events:
        if not event.overlaps(start, end):
            continue
        if requested_currencies or requested_symbols:
            currency_match = bool(
                set(requested_currencies).intersection(
                    event.affected_currencies
                )
            )
            symbol_match = bool(
                set(requested_symbols).intersection(event.affected_symbols)
            )
            if not (currency_match or symbol_match):
                continue
        if requested_kinds and event.kind not in requested_kinds:
            continue
        if selected_view is MarketContextView.EX_ANTE and (
            event.first_known_at_ns > cast(int, as_of)
            or event.available_at_ns > cast(int, as_of)
        ):
            hidden_by_availability = True
            continue
        matched.append(event)
        if len(matched) > limit:
            raise MarketContextQueryLimitError(
                "market context query exceeds configured event limit"
            )
    if matched:
        status = MarketContextQueryStatus.MATCHED
        missing_reason = None
    else:
        status = MarketContextQueryStatus.MISSING
        if (
            end <= timeline.coverage_start_ns
            or start >= timeline.coverage_end_ns
        ):
            missing_reason = (
                MarketContextMissingReason.OUTSIDE_TIMELINE_COVERAGE
            )
        elif hidden_by_availability:
            missing_reason = MarketContextMissingReason.NOT_AVAILABLE_AS_OF
        elif not timeline.complete:
            missing_reason = MarketContextMissingReason.TIMELINE_INCOMPLETE
        else:
            missing_reason = MarketContextMissingReason.NO_MATCHING_EVENT
    state = None
    if include_calendar:
        calendar_time = start if calendar_at_ns is None else calendar_at_ns
        if not start <= calendar_time < end:
            raise ValueError(
                "calendar_at_ns must lie inside the query interval"
            )
        state = market_context_calendar_state(
            calendar_time,
            calendar_profile=calendar_profile,
        )
    return MarketContextQueryV1(
        timeline_id=timeline.timeline_id,
        view=selected_view,
        start_ns=start,
        end_ns=end,
        as_of_ns=as_of,
        events=tuple(matched),
        status=status,
        missing_reason=missing_reason,
        calendar_state=state,
        requested_currencies=requested_currencies,
        requested_symbols=requested_symbols,
        requested_kinds=requested_kinds,
        window_id=window_id,
        limitations=timeline.limitations,
    )


def query_market_context_window(
    timeline: MarketContextTimelineV1,
    window: ReconstructionWindowV1,
    *,
    view: MarketContextView,
    as_of_ns: int | None = None,
    currencies: Sequence[str] = (),
    kinds: Sequence[MarketContextKind] = (),
    include_calendar: bool = True,
    calendar_profile: HistDataCalendarProfile | None = None,
    max_events: int = MAX_MARKET_CONTEXT_QUERY_EVENTS,
) -> MarketContextQueryV1:
    """Join context to one streaming window without materializing row columns."""
    if not isinstance(window, ReconstructionWindowV1):
        raise ValueError("window must use the v1 reconstruction contract")
    selected_view = MarketContextView.from_value(view)
    effective_as_of = as_of_ns
    if selected_view is MarketContextView.EX_ANTE and effective_as_of is None:
        effective_as_of = window.core_start_ns
    return query_market_context(
        timeline,
        start_ns=window.core_start_ns,
        end_ns=window.core_end_ns,
        view=selected_view,
        as_of_ns=effective_as_of,
        currencies=tuple(currencies) + _currencies_for_symbols(window.symbols),
        symbols=window.symbols,
        kinds=kinds,
        include_calendar=include_calendar,
        calendar_at_ns=window.core_start_ns,
        calendar_profile=calendar_profile,
        max_events=max_events,
        window_id=window.window_id,
    )


def market_context_information_inputs(
    query: MarketContextQueryV1,
    *,
    run_id: str,
    used_at_ns: int,
    split_kind: InformationSplitKind | None = None,
) -> tuple[ReconstructionInformationInputV1, ...]:
    """Bind queried event vintages into the existing leakage-audit graph."""
    if not isinstance(query, MarketContextQueryV1):
        raise ValueError("query must use the v1 market-context contract")
    run = _required_text(run_id)
    used = _bounded_int64(used_at_ns, "used_at_ns")
    if query.view is MarketContextView.EX_ANTE and query.as_of_ns != used:
        raise ValueError(
            "ex-ante information use must equal the query as_of_ns"
        )
    by_event_id: dict[str, ReconstructionInformationInputV1] = {}
    results: list[ReconstructionInformationInputV1] = []
    for event in sorted(
        query.events,
        key=lambda item: (item.revision_sequence, item.event_id),
    ):
        supersedes_input_id = None
        if event.supersedes_event_id is not None:
            predecessor = by_event_id.get(event.supersedes_event_id)
            if predecessor is None:
                raise ValueError("query omits a required revision predecessor")
            supersedes_input_id = predecessor.input_id
        # The information graph records when this vintage became a usable
        # fact.  The target market-event time remains immutable in the context
        # event itself.  This distinction lets a published schedule be used
        # before the scheduled release without mislabeling it as realized
        # future information.
        information_event_time = event.available_at_ns
        allowed_lookahead = 0
        if query.view is MarketContextView.EX_POST:
            allowed_lookahead = max(0, information_event_time - used)
        value = ReconstructionInformationInputV1(
            run_id=run,
            artifact_id=f"{query.timeline_id}:{event.event_id}",
            information_mode=query.information_mode,
            input_kind=InformationInputKind.EXTERNAL,
            stage=InformationStage.NEWS_CONTEXT,
            scope=(
                InformationScope.REVISION
                if event.revision_sequence
                else InformationScope.POINT_IN_TIME
            ),
            event_time_ns=information_event_time,
            available_at_ns=event.available_at_ns,
            used_at_ns=used,
            observation_start_ns=information_event_time,
            observation_end_ns=information_event_time,
            vintage_id=event.vintage_id,
            reason=(
                "point-in-time market context "
                f"{event.canonical_key} revision {event.revision_sequence}; "
                f"target_event_time_ns={event.event_time_ns}"
            ),
            revision_sequence=event.revision_sequence,
            supersedes_input_id=supersedes_input_id,
            allowed_lookahead_ns=allowed_lookahead,
            split_kind=split_kind,
        )
        by_event_id[event.event_id] = value
        results.append(value)
    return tuple(results)


def _validate_timeline_events(
    events: Sequence[MarketContextEventV1],
    coverage_start_ns: int,
    coverage_end_ns: int,
) -> None:
    ids: set[str] = set()
    logical: set[tuple[str, int]] = set()
    by_id: dict[str, MarketContextEventV1] = {}
    for event in events:
        if not coverage_start_ns <= event.event_time_ns < coverage_end_ns:
            raise ValueError("event time lies outside timeline coverage")
        if event.event_id in ids:
            raise ValueError("duplicate market context event_id")
        key = (event.canonical_key, event.revision_sequence)
        if key in logical:
            raise ValueError("duplicate logical market context event")
        ids.add(event.event_id)
        logical.add(key)
        by_id[event.event_id] = event
    for event in events:
        if event.revision_sequence == 0:
            continue
        predecessor = by_id.get(cast(str, event.supersedes_event_id))
        if predecessor is None:
            raise ValueError(
                "revised event predecessor is absent from timeline"
            )
        if predecessor.canonical_key != event.canonical_key:
            raise ValueError("revision changes canonical event identity")
        if predecessor.revision_sequence + 1 != event.revision_sequence:
            raise ValueError("revision sequence must advance by exactly one")
        if predecessor.event_time_ns != event.event_time_ns:
            raise ValueError("revision changes semantic event time")
        if predecessor.first_known_at_ns != event.first_known_at_ns:
            raise ValueError("revision changes first-known time")
        if predecessor.available_at_ns >= event.available_at_ns:
            raise ValueError("revision availability must advance")


def _verify_adapter_event(
    adapter: MarketContextSourceAdapterV1,
    event: MarketContextEventV1,
) -> None:
    if event.source.adapter_name != adapter.adapter_name:
        raise ValueError("event source adapter_name does not match adapter")
    if event.source.adapter_version != adapter.adapter_version:
        raise ValueError("event source adapter_version does not match adapter")


def _enum_value(
    enum_type: type[_EnumT],
    value: str | _EnumT,
    label: str,
) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).strip().lower())
    except ValueError as err:
        raise ValueError(f"unsupported {label}") from err


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = str(canonical_contract_json(payload)).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _canonical_key(value: Any) -> str:
    normalized = _required_text(value).lower()
    if not _CANONICAL_KEY_RE.fullmatch(normalized):
        raise ValueError("canonical_key contains unsupported characters")
    return normalized


def _required_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("required text is empty")
    if len(text) > MAX_MARKET_CONTEXT_TEXT:
        raise ValueError("text exceeds market-context limit")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return _required_text(text) if text else None


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _strict_int(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return value


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return _strict_int(value, "optional integer")


def _optional_fold(value: Any) -> int | None:
    if value is None:
        return None
    fold = _strict_int(value, "source_time_fold")
    if fold not in (0, 1):
        raise ValueError("source_time_fold must be zero or one")
    return fold


def _bounded_int64(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if not INT64_MIN <= result <= INT64_MAX:
        raise ValueError(f"{name} exceeds signed int64")
    return result


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ValueError(f"{name} must be nonnegative")
    return result


def _positive_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _nonnegative_int64(value: Any, name: str) -> int:
    result = _bounded_int64(value, name)
    if result < 0:
        raise ValueError(f"{name} must be nonnegative")
    return result


def _positive_int64(value: Any, name: str) -> int:
    result = _bounded_int64(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"{name} must be numeric") from err
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _optional_finite(value: Any, name: str) -> float | None:
    if value is None:
        return None
    return _finite_float(value, name)


def _optional_number(value: Any) -> float | None:
    return None if value is None else _finite_float(value, "optional number")


def _bounded_text_tuple(values: Iterable[Any], name: str) -> tuple[str, ...]:
    result = tuple(dict.fromkeys(_required_text(value) for value in values))
    if len(result) > MAX_MARKET_CONTEXT_ITEMS:
        raise ValueError(f"{name} exceeds market-context item limit")
    return result


def _normalized_currencies(values: Iterable[Any]) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(value).upper() for value in values}))
    if len(result) > MAX_MARKET_CONTEXT_ITEMS:
        raise ValueError("affected currencies exceed v1 limit")
    if any(not _CURRENCY_RE.fullmatch(value) for value in result):
        raise ValueError("affected currency must be a three-letter code")
    return result


def _normalized_symbols(values: Iterable[Any]) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(value).upper() for value in values}))
    if len(result) > MAX_MARKET_CONTEXT_ITEMS:
        raise ValueError("affected symbols exceed v1 limit")
    if any(not _SYMBOL_RE.fullmatch(value) for value in result):
        raise ValueError("affected symbol contains unsupported characters")
    return result


def _currencies_for_symbols(values: Iterable[str]) -> tuple[str, ...]:
    currencies: set[str] = set()
    for value in values:
        symbol = str(value).strip().upper()
        if len(symbol) == 6 and symbol.isalpha():
            currencies.update((symbol[:3], symbol[3:]))
    return tuple(sorted(currencies))


def _required_sha256(value: Any, name: str) -> str:
    text = _required_text(value).lower()
    if not _SHA256_RE.fullmatch(text):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return text


def _validate_json_value(value: Any, path: str) -> None:
    if value is None or isinstance(value, (str, int, bool)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite float")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} contains a non-string key")
            _validate_json_value(item, f"{path}.{key}")
        return
    raise ValueError(f"{path} is not JSON-compatible")


def _ensure_payload_size(value: Mapping[str, JSONValue], maximum: int) -> None:
    encoded = str(canonical_contract_json(value)).encode("utf-8")
    if len(encoded) > maximum:
        raise ValueError("market-context payload exceeds v1 size limit")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if str(data.get("schema_version", "")) != expected:
        raise ValueError("unsupported market-context schema version")


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError("expected sequence")
    return value


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(_mapping(item) for item in _sequence(value))


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as err:
        raise ValueError("invalid market-context JSON") from err
    return _mapping(value)
