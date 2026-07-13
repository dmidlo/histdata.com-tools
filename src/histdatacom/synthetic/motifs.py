"""Deterministic empirical reference-motif artifacts and retrieval.

The augmented tick row is an evidence surface, not a storage format for motif
records.  This module projects bounded source windows into compact event-time
offsets, bid/ask deltas, transition marks, conditioning coordinates, and
complete artifact/row lineage.  It also owns chronological split exclusion,
near-duplicate leakage refusal, deterministic fallback retrieval, and the
point-in-time bridge to the reconstruction information audit.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import math
import os
from pathlib import Path
import re
from typing import Any, cast

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.information import (
    InformationInputKind,
    InformationMode,
    InformationScope,
    InformationSplitKind,
    InformationStage,
    ReconstructionInformationInputV1,
)

REFERENCE_MOTIF_SPLIT_SCHEMA_VERSION = "histdatacom.reference-motif-split.v1"
REFERENCE_MOTIF_CONDITION_SCHEMA_VERSION = (
    "histdatacom.reference-motif-condition.v1"
)
REFERENCE_MOTIF_TRANSFORM_SCHEMA_VERSION = (
    "histdatacom.reference-motif-transform-policy.v1"
)
REFERENCE_MOTIF_SOURCE_EVENT_SCHEMA_VERSION = (
    "histdatacom.reference-motif-source-event.v1"
)
REFERENCE_MOTIF_SOURCE_WINDOW_SCHEMA_VERSION = (
    "histdatacom.reference-motif-source-window.v1"
)
REFERENCE_MOTIF_FRAGMENT_SCHEMA_VERSION = (
    "histdatacom.reference-motif-fragment.v1"
)
REFERENCE_MOTIF_INDEX_CONFIG_SCHEMA_VERSION = (
    "histdatacom.reference-motif-index-config.v1"
)
REFERENCE_MOTIF_INDEX_SCHEMA_VERSION = "histdatacom.reference-motif-index.v1"
REFERENCE_MOTIF_QUERY_SCHEMA_VERSION = "histdatacom.reference-motif-query.v1"
REFERENCE_MOTIF_BACKOFF_ATTEMPT_SCHEMA_VERSION = (
    "histdatacom.reference-motif-backoff-attempt.v1"
)
REFERENCE_MOTIF_MATCH_SCHEMA_VERSION = "histdatacom.reference-motif-match.v1"
REFERENCE_MOTIF_QUERY_RESULT_SCHEMA_VERSION = (
    "histdatacom.reference-motif-query-result.v1"
)
REFERENCE_MOTIF_ARTIFACT_KIND = "reference-motif-index"

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
MAX_REFERENCE_MOTIF_TEXT = 1024
MAX_REFERENCE_MOTIF_TAGS = 64
MAX_REFERENCE_MOTIF_EVENTS = 4096
MAX_REFERENCE_MOTIF_SOURCE_WINDOWS = 10_000
MAX_REFERENCE_MOTIF_FRAGMENTS = 4096
MAX_REFERENCE_MOTIF_MATCHES = 128
MAX_REFERENCE_MOTIF_EXCLUSIONS = 4096
MAX_REFERENCE_MOTIF_ARTIFACT_BYTES = 256 * 1024**2
MAX_REFERENCE_MOTIF_LEAKAGE_FINDINGS = 128

REFERENCE_MOTIF_METRIC_NAMES = (
    "return_value",
    "range_value",
    "volatility",
    "spread",
    "tick_intensity",
    "interarrival_ns",
    "timestamp_precision_ns",
    "price_precision_digits",
    "source_quality_score",
)
_NONNEGATIVE_METRICS = frozenset(REFERENCE_MOTIF_METRIC_NAMES) - {
    "return_value"
}
REFERENCE_MOTIF_BACKOFF_LEVELS = (
    "exact",
    "symbol_epoch_session_event",
    "symbol_epoch_session",
    "symbol_epoch_state",
    "symbol_epoch",
    "currency_epoch",
    "symbol",
    "epoch",
    "global",
)
_EXPECTED_SPLITS = (
    "train",
    "calibration",
    "validation",
    "final_holdout",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PERIOD_RE = re.compile(r"^\d{6}$")


class ReferenceMotifSplitKind(str, Enum):
    """Chronological source roles for motif leakage control."""

    TRAIN = "train"
    CALIBRATION = "calibration"
    VALIDATION = "validation"
    FINAL_HOLDOUT = "final_holdout"

    @classmethod
    def from_value(
        cls, value: str | "ReferenceMotifSplitKind"
    ) -> "ReferenceMotifSplitKind":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ValueError("unsupported reference motif split kind") from err


class ReferenceMotifTransition(str, Enum):
    """Which quote marks changed from the preceding source event."""

    START = "start"
    UNCHANGED = "unchanged"
    BID = "bid"
    ASK = "ask"
    BOTH = "both"

    @classmethod
    def from_value(
        cls, value: str | "ReferenceMotifTransition"
    ) -> "ReferenceMotifTransition":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ValueError("unsupported reference motif transition") from err


class ReferenceMotifQueryStatus(str, Enum):
    """Whether deterministic retrieval found admissible evidence."""

    MATCHED = "matched"
    NO_SUPPORTED_CELL = "no_supported_cell"
    NOT_AVAILABLE_AS_OF = "not_available_as_of"


class ReferenceMotifLeakageError(ValueError):
    """Fail-closed cross-split overlap or near-duplicate evidence."""

    def __init__(self, findings: Sequence[Mapping[str, JSONValue]]) -> None:
        self.findings = tuple(dict(item) for item in findings)
        super().__init__(
            "reference motif leakage audit failed with "
            f"{len(self.findings)} finding(s)"
        )


class ReferenceMotifResourceLimitError(ValueError):
    """A bounded build, artifact, or query limit was exceeded."""


@dataclass(frozen=True, slots=True)
class ReferenceMotifSplitV1:
    """One half-open chronological source split."""

    kind: ReferenceMotifSplitKind
    start_ns: int
    end_ns: int
    split_id: str = ""
    schema_version: str = REFERENCE_MOTIF_SPLIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_SPLIT_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif split schema")
        object.__setattr__(
            self, "kind", ReferenceMotifSplitKind.from_value(self.kind)
        )
        start = _bounded_int64(self.start_ns, "start_ns")
        end = _bounded_int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("reference motif split end must follow start")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        expected = _stable_id("reference-motif-split", self.identity_payload())
        supplied = _optional_text(self.split_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reference motif split_id differs")
        object.__setattr__(self, "split_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "interval": "[start_ns,end_ns)",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "split_id": self.split_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReferenceMotifSplitV1":
        _require_schema(data, REFERENCE_MOTIF_SPLIT_SCHEMA_VERSION)
        return cls(
            kind=ReferenceMotifSplitKind.from_value(str(data.get("kind", ""))),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            split_id=str(data.get("split_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReferenceMotifConditionV1:
    """Categorical and numeric coordinates for fragment retrieval."""

    symbol: str
    feed_epoch_id: str
    session_state: str
    currencies: tuple[str, ...] = ()
    active_sessions: tuple[str, ...] = ()
    overlap_tags: tuple[str, ...] = ()
    special_tags: tuple[str, ...] = ()
    holiday_tags: tuple[str, ...] = ()
    event_tags: tuple[str, ...] = ()
    return_regime: str = "unknown"
    range_regime: str = "unknown"
    volatility_regime: str = "unknown"
    spread_regime: str = "unknown"
    activity_regime: str = "unknown"
    interarrival_regime: str = "unknown"
    timestamp_precision: str = "unknown"
    price_precision: str = "unknown"
    source_quality_state: str = "unknown"
    metrics: Mapping[str, float] = field(default_factory=dict)
    schema_version: str = REFERENCE_MOTIF_CONDITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_CONDITION_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif condition schema")
        symbol = _normalized_symbol(self.symbol)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(
            self, "feed_epoch_id", _required_text(self.feed_epoch_id)
        )
        object.__setattr__(
            self, "session_state", _required_text(self.session_state)
        )
        currencies = _normalized_currencies(
            self.currencies or _symbol_currencies(symbol)
        )
        object.__setattr__(self, "currencies", currencies)
        for name in (
            "active_sessions",
            "overlap_tags",
            "special_tags",
            "holiday_tags",
            "event_tags",
        ):
            object.__setattr__(
                self,
                name,
                _normalized_text_tuple(
                    getattr(self, name), maximum=MAX_REFERENCE_MOTIF_TAGS
                ),
            )
        for name in (
            "return_regime",
            "range_regime",
            "volatility_regime",
            "spread_regime",
            "activity_regime",
            "interarrival_regime",
            "timestamp_precision",
            "price_precision",
            "source_quality_state",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        metrics = _metric_mapping(self.metrics)
        object.__setattr__(self, "metrics", metrics)

    def pattern_for_level(self, level: str) -> dict[str, str] | None:
        """Return an explicit categorical key pattern for one fallback level."""
        if level not in REFERENCE_MOTIF_BACKOFF_LEVELS:
            raise ValueError("unsupported reference motif backoff level")
        if level == "global":
            return {}
        if level == "epoch":
            return {"feed_epoch_id": self.feed_epoch_id}
        if level == "symbol":
            return {"symbol": self.symbol}
        if level == "currency_epoch":
            return {
                "currencies": ",".join(self.currencies),
                "feed_epoch_id": self.feed_epoch_id,
            }
        base = {"symbol": self.symbol, "feed_epoch_id": self.feed_epoch_id}
        if level == "symbol_epoch":
            return base
        if level == "symbol_epoch_state":
            return {
                **base,
                "return_regime": self.return_regime,
                "volatility_regime": self.volatility_regime,
                "spread_regime": self.spread_regime,
                "activity_regime": self.activity_regime,
            }
        session = {
            **base,
            "session_state": self.session_state,
            "active_sessions": ",".join(self.active_sessions),
        }
        if level == "symbol_epoch_session":
            return session
        if level == "symbol_epoch_session_event":
            return {**session, "event_tags": ",".join(self.event_tags)}
        return self.coordinates()

    def matches(self, pattern: Mapping[str, str]) -> bool:
        coordinates = self.coordinates()
        return all(
            coordinates.get(name) == value for name, value in pattern.items()
        )

    def coordinates(self) -> dict[str, str]:
        """Return every categorical coordinate in comparison form."""
        return {
            "symbol": self.symbol,
            "currencies": ",".join(self.currencies),
            "feed_epoch_id": self.feed_epoch_id,
            "session_state": self.session_state,
            "active_sessions": ",".join(self.active_sessions),
            "overlap_tags": ",".join(self.overlap_tags),
            "special_tags": ",".join(self.special_tags),
            "holiday_tags": ",".join(self.holiday_tags),
            "event_tags": ",".join(self.event_tags),
            "return_regime": self.return_regime,
            "range_regime": self.range_regime,
            "volatility_regime": self.volatility_regime,
            "spread_regime": self.spread_regime,
            "activity_regime": self.activity_regime,
            "interarrival_regime": self.interarrival_regime,
            "timestamp_precision": self.timestamp_precision,
            "price_precision": self.price_precision,
            "source_quality_state": self.source_quality_state,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            **self.coordinates(),
            "currencies": list(self.currencies),
            "active_sessions": list(self.active_sessions),
            "overlap_tags": list(self.overlap_tags),
            "special_tags": list(self.special_tags),
            "holiday_tags": list(self.holiday_tags),
            "event_tags": list(self.event_tags),
            "metrics": dict(self.metrics),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReferenceMotifConditionV1":
        _require_schema(data, REFERENCE_MOTIF_CONDITION_SCHEMA_VERSION)
        return cls(
            symbol=str(data.get("symbol", "")),
            feed_epoch_id=str(data.get("feed_epoch_id", "")),
            session_state=str(data.get("session_state", "")),
            currencies=_string_tuple(data.get("currencies")),
            active_sessions=_string_tuple(data.get("active_sessions")),
            overlap_tags=_string_tuple(data.get("overlap_tags")),
            special_tags=_string_tuple(data.get("special_tags")),
            holiday_tags=_string_tuple(data.get("holiday_tags")),
            event_tags=_string_tuple(data.get("event_tags")),
            return_regime=str(data.get("return_regime", "")),
            range_regime=str(data.get("range_regime", "")),
            volatility_regime=str(data.get("volatility_regime", "")),
            spread_regime=str(data.get("spread_regime", "")),
            activity_regime=str(data.get("activity_regime", "")),
            interarrival_regime=str(data.get("interarrival_regime", "")),
            timestamp_precision=str(data.get("timestamp_precision", "")),
            price_precision=str(data.get("price_precision", "")),
            source_quality_state=str(data.get("source_quality_state", "")),
            metrics={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(data.get("metrics")).items()
            },
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReferenceMotifTransformPolicyV1:
    """Admissible fragment scaling and seam-warp envelope."""

    min_time_scale: float = 0.5
    max_time_scale: float = 2.0
    min_price_scale: float = 0.5
    max_price_scale: float = 2.0
    max_time_warp_ratio: float = 1.25
    allow_price_translation: bool = True
    allow_spread_scaling: bool = False
    policy_id: str = ""
    schema_version: str = REFERENCE_MOTIF_TRANSFORM_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_TRANSFORM_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif transform schema")
        for prefix in ("time", "price"):
            low = _positive_float(
                getattr(self, f"min_{prefix}_scale"), f"min_{prefix}_scale"
            )
            high = _positive_float(
                getattr(self, f"max_{prefix}_scale"), f"max_{prefix}_scale"
            )
            if not low <= 1.0 <= high:
                raise ValueError(f"{prefix} scale range must contain one")
            object.__setattr__(self, f"min_{prefix}_scale", low)
            object.__setattr__(self, f"max_{prefix}_scale", high)
        warp = _finite_float(self.max_time_warp_ratio, "max_time_warp_ratio")
        if warp < 1.0:
            raise ValueError("max_time_warp_ratio must be at least one")
        object.__setattr__(self, "max_time_warp_ratio", warp)
        _strict_bool(self.allow_price_translation, "allow_price_translation")
        _strict_bool(self.allow_spread_scaling, "allow_spread_scaling")
        expected = _stable_id(
            "reference-motif-transform", self.identity_payload()
        )
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reference motif transform policy_id differs")
        object.__setattr__(self, "policy_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "min_time_scale": self.min_time_scale,
            "max_time_scale": self.max_time_scale,
            "min_price_scale": self.min_price_scale,
            "max_price_scale": self.max_price_scale,
            "max_time_warp_ratio": self.max_time_warp_ratio,
            "allow_price_translation": self.allow_price_translation,
            "allow_spread_scaling": self.allow_spread_scaling,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReferenceMotifTransformPolicyV1":
        _require_schema(data, REFERENCE_MOTIF_TRANSFORM_SCHEMA_VERSION)
        return cls(
            min_time_scale=_finite_float(
                data.get("min_time_scale"), "min_time_scale"
            ),
            max_time_scale=_finite_float(
                data.get("max_time_scale"), "max_time_scale"
            ),
            min_price_scale=_finite_float(
                data.get("min_price_scale"), "min_price_scale"
            ),
            max_price_scale=_finite_float(
                data.get("max_price_scale"), "max_price_scale"
            ),
            max_time_warp_ratio=_finite_float(
                data.get("max_time_warp_ratio"), "max_time_warp_ratio"
            ),
            allow_price_translation=_strict_bool(
                data.get("allow_price_translation"), "allow_price_translation"
            ),
            allow_spread_scaling=_strict_bool(
                data.get("allow_spread_scaling"), "allow_spread_scaling"
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReferenceMotifSourceEventV1:
    """One observed event projected from the augmented training surface."""

    event_time_ns: int
    event_sequence: int
    bid: float
    ask: float
    source_row_id: int
    source_event_id: str = ""
    schema_version: str = REFERENCE_MOTIF_SOURCE_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_SOURCE_EVENT_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif source event schema")
        object.__setattr__(
            self,
            "event_time_ns",
            _bounded_int64(self.event_time_ns, "event_time_ns"),
        )
        sequence = _nonnegative_int(self.event_sequence, "event_sequence")
        row_id = _nonnegative_int(self.source_row_id, "source_row_id")
        bid = _positive_float(self.bid, "bid")
        ask = _positive_float(self.ask, "ask")
        if ask < bid:
            raise ValueError("reference motif source event has negative spread")
        object.__setattr__(self, "event_sequence", sequence)
        object.__setattr__(self, "source_row_id", row_id)
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        expected = _stable_id(
            "reference-motif-source-event", self.identity_payload()
        )
        supplied = _optional_text(self.source_event_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reference motif source_event_id differs")
        object.__setattr__(self, "source_event_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "event_time_ns": self.event_time_ns,
            "event_sequence": self.event_sequence,
            "bid": self.bid,
            "ask": self.ask,
            "source_row_id": self.source_row_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "source_event_id": self.source_event_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReferenceMotifSourceEventV1":
        _require_schema(data, REFERENCE_MOTIF_SOURCE_EVENT_SCHEMA_VERSION)
        return cls(
            event_time_ns=_strict_int(
                data.get("event_time_ns"), "event_time_ns"
            ),
            event_sequence=_strict_int(
                data.get("event_sequence"), "event_sequence"
            ),
            bid=_finite_float(data.get("bid"), "bid"),
            ask=_finite_float(data.get("ask"), "ask"),
            source_row_id=_strict_int(
                data.get("source_row_id"), "source_row_id"
            ),
            source_event_id=str(data.get("source_event_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReferenceMotifSourceWindowV1:
    """One bounded observed window before compact motif projection."""

    source_series_id: str
    period: str
    source_artifact: ArtifactRef
    split_kind: ReferenceMotifSplitKind
    condition: ReferenceMotifConditionV1
    events: tuple[ReferenceMotifSourceEventV1, ...]
    first_known_at_ns: int
    available_at_ns: int
    data_quality_eligible: bool = True
    data_quality_reasons: tuple[str, ...] = ()
    transform_policy: ReferenceMotifTransformPolicyV1 = field(
        default_factory=ReferenceMotifTransformPolicyV1
    )
    source_window_id: str = ""
    schema_version: str = REFERENCE_MOTIF_SOURCE_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_SOURCE_WINDOW_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif source window schema")
        object.__setattr__(
            self, "source_series_id", _required_text(self.source_series_id)
        )
        period = _required_text(self.period)
        if not _PERIOD_RE.fullmatch(period):
            raise ValueError("reference motif period must use YYYYMM")
        object.__setattr__(self, "period", period)
        object.__setattr__(
            self,
            "source_artifact",
            _validated_artifact_ref(self.source_artifact),
        )
        object.__setattr__(
            self,
            "split_kind",
            ReferenceMotifSplitKind.from_value(self.split_kind),
        )
        if not isinstance(self.condition, ReferenceMotifConditionV1):
            raise ValueError(
                "reference motif source window requires a v1 condition"
            )
        events = tuple(self.events)
        if not 2 <= len(events) <= MAX_REFERENCE_MOTIF_EVENTS:
            raise ValueError(
                "reference motif source window event count is outside limits"
            )
        if any(
            not isinstance(item, ReferenceMotifSourceEventV1) for item in events
        ):
            raise ValueError("reference motif source window requires v1 events")
        positions = [
            (item.event_time_ns, item.event_sequence) for item in events
        ]
        if positions != sorted(positions) or len(set(positions)) != len(
            positions
        ):
            raise ValueError(
                "reference motif source events must be uniquely ordered"
            )
        if events[-1].event_time_ns <= events[0].event_time_ns:
            raise ValueError(
                "reference motif source window requires positive duration"
            )
        if len({item.source_row_id for item in events}) != len(events):
            raise ValueError(
                "reference motif source window has duplicate row lineage"
            )
        if self.condition.symbol not in self.source_series_id.upper():
            raise ValueError(
                "source series identity does not contain condition symbol"
            )
        object.__setattr__(self, "events", events)
        first_known = _bounded_int64(
            self.first_known_at_ns, "first_known_at_ns"
        )
        available = _bounded_int64(self.available_at_ns, "available_at_ns")
        if first_known < events[-1].event_time_ns:
            raise ValueError(
                "source window cannot be known before its last observation"
            )
        if available < first_known:
            raise ValueError(
                "source window available_at precedes first_known_at"
            )
        object.__setattr__(self, "first_known_at_ns", first_known)
        object.__setattr__(self, "available_at_ns", available)
        eligible = _strict_bool(
            self.data_quality_eligible, "data_quality_eligible"
        )
        reasons = _normalized_text_tuple(
            self.data_quality_reasons, maximum=MAX_REFERENCE_MOTIF_TAGS
        )
        if eligible and reasons:
            raise ValueError(
                "eligible motif source cannot carry quality exclusions"
            )
        if not eligible and not reasons:
            raise ValueError("ineligible motif source requires quality reasons")
        object.__setattr__(self, "data_quality_reasons", reasons)
        if not isinstance(
            self.transform_policy, ReferenceMotifTransformPolicyV1
        ):
            raise ValueError(
                "reference motif source requires a v1 transform policy"
            )
        expected = _stable_id(
            "reference-motif-source-window", self.identity_payload()
        )
        supplied = _optional_text(self.source_window_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reference motif source_window_id differs")
        object.__setattr__(self, "source_window_id", expected)

    @property
    def start_ns(self) -> int:
        return self.events[0].event_time_ns

    @property
    def end_ns(self) -> int:
        """Return the last observed timestamp (inclusive)."""
        return self.events[-1].event_time_ns

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_series_id": self.source_series_id,
            "period": self.period,
            "source_artifact": self.source_artifact.to_dict(),
            "split_kind": self.split_kind.value,
            "condition": self.condition.to_dict(),
            "event_ids": [item.source_event_id for item in self.events],
            "first_known_at_ns": self.first_known_at_ns,
            "available_at_ns": self.available_at_ns,
            "data_quality_eligible": self.data_quality_eligible,
            "data_quality_reasons": list(self.data_quality_reasons),
            "transform_policy": self.transform_policy.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "source_window_id": self.source_window_id,
            "events": [item.to_dict() for item in self.events],
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReferenceMotifSourceWindowV1":
        _require_schema(data, REFERENCE_MOTIF_SOURCE_WINDOW_SCHEMA_VERSION)
        return cls(
            source_series_id=str(data.get("source_series_id", "")),
            period=str(data.get("period", "")),
            source_artifact=ArtifactRef.from_dict(
                _mapping(data.get("source_artifact"))
            ),
            split_kind=ReferenceMotifSplitKind.from_value(
                str(data.get("split_kind", ""))
            ),
            condition=ReferenceMotifConditionV1.from_dict(
                _mapping(data.get("condition"))
            ),
            events=tuple(
                ReferenceMotifSourceEventV1.from_dict(item)
                for item in _mapping_sequence(data.get("events"))
            ),
            first_known_at_ns=_strict_int(
                data.get("first_known_at_ns"), "first_known_at_ns"
            ),
            available_at_ns=_strict_int(
                data.get("available_at_ns"), "available_at_ns"
            ),
            data_quality_eligible=_strict_bool(
                data.get("data_quality_eligible"), "data_quality_eligible"
            ),
            data_quality_reasons=_string_tuple(
                data.get("data_quality_reasons")
            ),
            transform_policy=ReferenceMotifTransformPolicyV1.from_dict(
                _mapping(data.get("transform_policy"))
            ),
            source_window_id=str(data.get("source_window_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReferenceMotifFragmentV1:
    """Compact empirical sequence with complete source-window lineage."""

    source_window_id: str
    source_series_id: str
    period: str
    source_artifact: ArtifactRef
    split_kind: ReferenceMotifSplitKind
    condition: ReferenceMotifConditionV1
    source_start_ns: int
    source_end_ns: int
    source_row_ids: tuple[int, ...]
    source_event_ids: tuple[str, ...]
    event_offsets_ns: tuple[int, ...]
    bid_deltas: tuple[float, ...]
    ask_deltas: tuple[float, ...]
    transitions: tuple[ReferenceMotifTransition, ...]
    start_bid: float
    start_ask: float
    first_known_at_ns: int
    available_at_ns: int
    data_quality_eligible: bool
    data_quality_reasons: tuple[str, ...]
    transform_policy: ReferenceMotifTransformPolicyV1
    near_duplicate_signature: str
    fragment_id: str = ""
    schema_version: str = REFERENCE_MOTIF_FRAGMENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_FRAGMENT_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif fragment schema")
        object.__setattr__(
            self,
            "source_window_id",
            _required_sha256_id(
                self.source_window_id,
                "source_window_id",
                "reference-motif-source-window",
            ),
        )
        object.__setattr__(
            self, "source_series_id", _required_text(self.source_series_id)
        )
        if not _PERIOD_RE.fullmatch(self.period):
            raise ValueError("reference motif fragment period must use YYYYMM")
        object.__setattr__(
            self,
            "source_artifact",
            _validated_artifact_ref(self.source_artifact),
        )
        object.__setattr__(
            self,
            "split_kind",
            ReferenceMotifSplitKind.from_value(self.split_kind),
        )
        if not isinstance(self.condition, ReferenceMotifConditionV1):
            raise ValueError("reference motif fragment requires a v1 condition")
        start = _bounded_int64(self.source_start_ns, "source_start_ns")
        end = _bounded_int64(self.source_end_ns, "source_end_ns")
        if end <= start:
            raise ValueError(
                "reference motif fragment requires positive duration"
            )
        object.__setattr__(self, "source_start_ns", start)
        object.__setattr__(self, "source_end_ns", end)
        row_ids = tuple(
            _nonnegative_int(item, "source_row_id")
            for item in self.source_row_ids
        )
        event_ids = tuple(
            _required_sha256_id(
                item, "source_event_id", "reference-motif-source-event"
            )
            for item in self.source_event_ids
        )
        offsets = tuple(
            _nonnegative_int64(item, "event_offset_ns")
            for item in self.event_offsets_ns
        )
        bids = tuple(
            _finite_float(item, "bid_delta") for item in self.bid_deltas
        )
        asks = tuple(
            _finite_float(item, "ask_delta") for item in self.ask_deltas
        )
        transitions = tuple(
            ReferenceMotifTransition.from_value(item)
            for item in self.transitions
        )
        sizes = {
            len(row_ids),
            len(event_ids),
            len(offsets),
            len(bids),
            len(asks),
            len(transitions),
        }
        if (
            len(sizes) != 1
            or not 2 <= len(offsets) <= MAX_REFERENCE_MOTIF_EVENTS
        ):
            raise ValueError("reference motif compact sequence lengths differ")
        if offsets[0] != 0 or offsets[-1] != end - start:
            raise ValueError(
                "reference motif offsets do not preserve boundaries"
            )
        if list(offsets) != sorted(offsets):
            raise ValueError("reference motif offsets must be ordered")
        if bids[0] != 0.0 or asks[0] != 0.0:
            raise ValueError("reference motif deltas must begin at zero")
        if transitions[0] is not ReferenceMotifTransition.START:
            raise ValueError("reference motif sequence must begin with start")
        expected_transitions = (ReferenceMotifTransition.START,) + tuple(
            _delta_transition(
                bids[index - 1],
                asks[index - 1],
                bids[index],
                asks[index],
            )
            for index in range(1, len(bids))
        )
        if transitions != expected_transitions:
            raise ValueError(
                "reference motif transition marks disagree with quote deltas"
            )
        object.__setattr__(self, "source_row_ids", row_ids)
        object.__setattr__(self, "source_event_ids", event_ids)
        object.__setattr__(self, "event_offsets_ns", offsets)
        object.__setattr__(self, "bid_deltas", bids)
        object.__setattr__(self, "ask_deltas", asks)
        object.__setattr__(self, "transitions", transitions)
        start_bid = _positive_float(self.start_bid, "start_bid")
        start_ask = _positive_float(self.start_ask, "start_ask")
        if start_ask < start_bid:
            raise ValueError(
                "reference motif fragment has negative start spread"
            )
        reconstructed_quotes = tuple(
            (start_bid + bid_delta, start_ask + ask_delta)
            for bid_delta, ask_delta in zip(bids, asks)
        )
        if any(bid <= 0.0 or ask <= 0.0 for bid, ask in reconstructed_quotes):
            raise ValueError(
                "reference motif fragment reconstructs a non-positive quote"
            )
        if any(ask < bid for bid, ask in reconstructed_quotes):
            raise ValueError(
                "reference motif fragment reconstructs a negative spread"
            )
        object.__setattr__(self, "start_bid", start_bid)
        object.__setattr__(self, "start_ask", start_ask)
        first_known = _bounded_int64(
            self.first_known_at_ns, "first_known_at_ns"
        )
        available = _bounded_int64(self.available_at_ns, "available_at_ns")
        if first_known < end:
            raise ValueError(
                "reference motif fragment cannot be known before its end"
            )
        if available < first_known:
            raise ValueError("reference motif fragment availability regresses")
        object.__setattr__(self, "first_known_at_ns", first_known)
        object.__setattr__(self, "available_at_ns", available)
        eligible = _strict_bool(
            self.data_quality_eligible, "data_quality_eligible"
        )
        reasons = _normalized_text_tuple(
            self.data_quality_reasons, maximum=MAX_REFERENCE_MOTIF_TAGS
        )
        if eligible == bool(reasons):
            raise ValueError(
                "fragment quality eligibility and reasons disagree"
            )
        object.__setattr__(self, "data_quality_reasons", reasons)
        if not isinstance(
            self.transform_policy, ReferenceMotifTransformPolicyV1
        ):
            raise ValueError("fragment requires a v1 transform policy")
        object.__setattr__(
            self,
            "near_duplicate_signature",
            _required_sha256_id(
                self.near_duplicate_signature,
                "near_duplicate_signature",
                "reference-motif-shape",
            ),
        )
        expected = _stable_id(
            "reference-motif-fragment", self.identity_payload()
        )
        supplied = _optional_text(self.fragment_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reference motif fragment_id differs")
        object.__setattr__(self, "fragment_id", expected)

    @property
    def duration_ns(self) -> int:
        return self.source_end_ns - self.source_start_ns

    @property
    def end_bid(self) -> float:
        return self.start_bid + self.bid_deltas[-1]

    @property
    def end_ask(self) -> float:
        return self.start_ask + self.ask_deltas[-1]

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_window_id": self.source_window_id,
            "source_series_id": self.source_series_id,
            "period": self.period,
            "source_artifact": self.source_artifact.to_dict(),
            "split_kind": self.split_kind.value,
            "condition": self.condition.to_dict(),
            "source_start_ns": self.source_start_ns,
            "source_end_ns": self.source_end_ns,
            "source_row_ids": list(self.source_row_ids),
            "source_event_ids": list(self.source_event_ids),
            "event_offsets_ns": list(self.event_offsets_ns),
            "bid_deltas": list(self.bid_deltas),
            "ask_deltas": list(self.ask_deltas),
            "transitions": [item.value for item in self.transitions],
            "start_bid": self.start_bid,
            "start_ask": self.start_ask,
            "first_known_at_ns": self.first_known_at_ns,
            "available_at_ns": self.available_at_ns,
            "data_quality_eligible": self.data_quality_eligible,
            "data_quality_reasons": list(self.data_quality_reasons),
            "transform_policy": self.transform_policy.to_dict(),
            "near_duplicate_signature": self.near_duplicate_signature,
            "sequence_layout": "compact-offsets-and-deltas-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "fragment_id": self.fragment_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReferenceMotifFragmentV1":
        _require_schema(data, REFERENCE_MOTIF_FRAGMENT_SCHEMA_VERSION)
        return cls(
            source_window_id=str(data.get("source_window_id", "")),
            source_series_id=str(data.get("source_series_id", "")),
            period=str(data.get("period", "")),
            source_artifact=ArtifactRef.from_dict(
                _mapping(data.get("source_artifact"))
            ),
            split_kind=ReferenceMotifSplitKind.from_value(
                str(data.get("split_kind", ""))
            ),
            condition=ReferenceMotifConditionV1.from_dict(
                _mapping(data.get("condition"))
            ),
            source_start_ns=_strict_int(
                data.get("source_start_ns"), "source_start_ns"
            ),
            source_end_ns=_strict_int(
                data.get("source_end_ns"), "source_end_ns"
            ),
            source_row_ids=_int_tuple(data.get("source_row_ids")),
            source_event_ids=_string_tuple(data.get("source_event_ids")),
            event_offsets_ns=_int_tuple(data.get("event_offsets_ns")),
            bid_deltas=_float_tuple(data.get("bid_deltas")),
            ask_deltas=_float_tuple(data.get("ask_deltas")),
            transitions=tuple(
                ReferenceMotifTransition.from_value(item)
                for item in _string_tuple(data.get("transitions"))
            ),
            start_bid=_finite_float(data.get("start_bid"), "start_bid"),
            start_ask=_finite_float(data.get("start_ask"), "start_ask"),
            first_known_at_ns=_strict_int(
                data.get("first_known_at_ns"), "first_known_at_ns"
            ),
            available_at_ns=_strict_int(
                data.get("available_at_ns"), "available_at_ns"
            ),
            data_quality_eligible=_strict_bool(
                data.get("data_quality_eligible"), "data_quality_eligible"
            ),
            data_quality_reasons=_string_tuple(
                data.get("data_quality_reasons")
            ),
            transform_policy=ReferenceMotifTransformPolicyV1.from_dict(
                _mapping(data.get("transform_policy"))
            ),
            near_duplicate_signature=str(
                data.get("near_duplicate_signature", "")
            ),
            fragment_id=str(data.get("fragment_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _default_metric_scales() -> dict[str, float]:
    return {
        "return_value": 0.001,
        "range_value": 0.001,
        "volatility": 0.001,
        "spread": 0.0001,
        "tick_intensity": 1.0,
        "interarrival_ns": 1_000_000_000.0,
        "timestamp_precision_ns": 1_000_000.0,
        "price_precision_digits": 1.0,
        "source_quality_score": 1.0,
    }


def _default_metric_weights() -> dict[str, float]:
    return {name: 1.0 for name in REFERENCE_MOTIF_METRIC_NAMES}


@dataclass(frozen=True, slots=True)
class ReferenceMotifIndexConfigV1:
    """Bounded extraction, leakage, selection, and retrieval policy."""

    min_events_per_fragment: int = 3
    max_events_per_fragment: int = MAX_REFERENCE_MOTIF_EVENTS
    max_source_windows: int = MAX_REFERENCE_MOTIF_SOURCE_WINDOWS
    max_fragments: int = MAX_REFERENCE_MOTIF_FRAGMENTS
    min_cell_support: int = 2
    max_matches: int = 32
    source_overlap_guard_ns: int = 0
    near_duplicate_rounding_digits: int = 4
    rounding_digits: int = 12
    max_artifact_bytes: int = MAX_REFERENCE_MOTIF_ARTIFACT_BYTES
    categorical_mismatch_penalty: float = 0.25
    missing_metric_penalty: float = 1.0
    metric_scales: Mapping[str, float] = field(
        default_factory=_default_metric_scales
    )
    metric_weights: Mapping[str, float] = field(
        default_factory=_default_metric_weights
    )
    backoff_levels: tuple[str, ...] = REFERENCE_MOTIF_BACKOFF_LEVELS
    config_id: str = ""
    schema_version: str = REFERENCE_MOTIF_INDEX_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_INDEX_CONFIG_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif index config schema")
        minimum = _bounded_int(
            self.min_events_per_fragment,
            "min_events_per_fragment",
            2,
            MAX_REFERENCE_MOTIF_EVENTS,
        )
        maximum = _bounded_int(
            self.max_events_per_fragment,
            "max_events_per_fragment",
            minimum,
            MAX_REFERENCE_MOTIF_EVENTS,
        )
        object.__setattr__(self, "min_events_per_fragment", minimum)
        object.__setattr__(self, "max_events_per_fragment", maximum)
        object.__setattr__(
            self,
            "max_source_windows",
            _bounded_int(
                self.max_source_windows,
                "max_source_windows",
                1,
                MAX_REFERENCE_MOTIF_SOURCE_WINDOWS,
            ),
        )
        object.__setattr__(
            self,
            "max_fragments",
            _bounded_int(
                self.max_fragments,
                "max_fragments",
                1,
                MAX_REFERENCE_MOTIF_FRAGMENTS,
            ),
        )
        object.__setattr__(
            self,
            "min_cell_support",
            _bounded_int(
                self.min_cell_support,
                "min_cell_support",
                1,
                MAX_REFERENCE_MOTIF_FRAGMENTS,
            ),
        )
        object.__setattr__(
            self,
            "max_matches",
            _bounded_int(
                self.max_matches, "max_matches", 1, MAX_REFERENCE_MOTIF_MATCHES
            ),
        )
        object.__setattr__(
            self,
            "source_overlap_guard_ns",
            _nonnegative_int64(
                self.source_overlap_guard_ns, "source_overlap_guard_ns"
            ),
        )
        for name in ("near_duplicate_rounding_digits", "rounding_digits"):
            value = _bounded_int(getattr(self, name), name, 0, 16)
            object.__setattr__(self, name, value)
        object.__setattr__(
            self,
            "max_artifact_bytes",
            _bounded_int(
                self.max_artifact_bytes,
                "max_artifact_bytes",
                1024,
                MAX_REFERENCE_MOTIF_ARTIFACT_BYTES,
            ),
        )
        for name in ("categorical_mismatch_penalty", "missing_metric_penalty"):
            penalty = _nonnegative_float(getattr(self, name), name)
            object.__setattr__(self, name, penalty)
        scales = _named_positive_mapping(self.metric_scales, "metric_scales")
        weights = _named_nonnegative_mapping(
            self.metric_weights, "metric_weights"
        )
        object.__setattr__(self, "metric_scales", scales)
        object.__setattr__(self, "metric_weights", weights)
        levels = tuple(dict.fromkeys(str(item) for item in self.backoff_levels))
        if (
            not levels
            or levels[-1] != "global"
            or any(
                item not in REFERENCE_MOTIF_BACKOFF_LEVELS for item in levels
            )
        ):
            raise ValueError(
                "reference motif fallback must be supported and end in global"
            )
        object.__setattr__(self, "backoff_levels", levels)
        expected = _stable_id(
            "reference-motif-index-config", self.identity_payload()
        )
        supplied = _optional_text(self.config_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reference motif config_id differs")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "min_events_per_fragment": self.min_events_per_fragment,
            "max_events_per_fragment": self.max_events_per_fragment,
            "max_source_windows": self.max_source_windows,
            "max_fragments": self.max_fragments,
            "min_cell_support": self.min_cell_support,
            "max_matches": self.max_matches,
            "source_overlap_guard_ns": self.source_overlap_guard_ns,
            "near_duplicate_rounding_digits": (
                self.near_duplicate_rounding_digits
            ),
            "rounding_digits": self.rounding_digits,
            "max_artifact_bytes": self.max_artifact_bytes,
            "categorical_mismatch_penalty": self.categorical_mismatch_penalty,
            "missing_metric_penalty": self.missing_metric_penalty,
            "metric_scales": dict(self.metric_scales),
            "metric_weights": dict(self.metric_weights),
            "backoff_levels": list(self.backoff_levels),
            "indexable_split": ReferenceMotifSplitKind.TRAIN.value,
            "cross_split_near_duplicate_policy": "fail_closed",
            "bounded_selection": "stable_hash_priority",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "config_id": self.config_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReferenceMotifIndexConfigV1":
        _require_schema(data, REFERENCE_MOTIF_INDEX_CONFIG_SCHEMA_VERSION)
        return cls(
            min_events_per_fragment=_strict_int(
                data.get("min_events_per_fragment"), "min_events_per_fragment"
            ),
            max_events_per_fragment=_strict_int(
                data.get("max_events_per_fragment"), "max_events_per_fragment"
            ),
            max_source_windows=_strict_int(
                data.get("max_source_windows"), "max_source_windows"
            ),
            max_fragments=_strict_int(
                data.get("max_fragments"), "max_fragments"
            ),
            min_cell_support=_strict_int(
                data.get("min_cell_support"), "min_cell_support"
            ),
            max_matches=_strict_int(data.get("max_matches"), "max_matches"),
            source_overlap_guard_ns=_strict_int(
                data.get("source_overlap_guard_ns"), "source_overlap_guard_ns"
            ),
            near_duplicate_rounding_digits=_strict_int(
                data.get("near_duplicate_rounding_digits"),
                "near_duplicate_rounding_digits",
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            max_artifact_bytes=_strict_int(
                data.get("max_artifact_bytes"), "max_artifact_bytes"
            ),
            categorical_mismatch_penalty=_finite_float(
                data.get("categorical_mismatch_penalty"),
                "categorical_mismatch_penalty",
            ),
            missing_metric_penalty=_finite_float(
                data.get("missing_metric_penalty"), "missing_metric_penalty"
            ),
            metric_scales={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(data.get("metric_scales")).items()
            },
            metric_weights={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(data.get("metric_weights")).items()
            },
            backoff_levels=_string_tuple(data.get("backoff_levels")),
            config_id=str(data.get("config_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def extract_reference_motif_fragment(
    window: ReferenceMotifSourceWindowV1,
    *,
    config: ReferenceMotifIndexConfigV1 | None = None,
) -> ReferenceMotifFragmentV1:
    """Project one source window into compact offsets, deltas, and marks."""
    if not isinstance(window, ReferenceMotifSourceWindowV1):
        raise ValueError("motif extraction requires a v1 source window")
    selected = config or ReferenceMotifIndexConfigV1()
    if (
        not selected.min_events_per_fragment
        <= len(window.events)
        <= selected.max_events_per_fragment
    ):
        raise ReferenceMotifResourceLimitError(
            "source window event count is outside configured fragment limits"
        )
    first = window.events[0]
    offsets = tuple(
        item.event_time_ns - first.event_time_ns for item in window.events
    )
    bids = tuple(
        round(item.bid - first.bid, selected.rounding_digits)
        for item in window.events
    )
    asks = tuple(
        round(item.ask - first.ask, selected.rounding_digits)
        for item in window.events
    )
    transitions = (ReferenceMotifTransition.START,) + tuple(
        _transition(previous, current)
        for previous, current in zip(window.events, window.events[1:])
    )
    signature = _near_duplicate_signature(
        symbol=window.condition.symbol,
        offsets=offsets,
        bid_deltas=bids,
        ask_deltas=asks,
        transitions=transitions,
        digits=selected.near_duplicate_rounding_digits,
    )
    return ReferenceMotifFragmentV1(
        source_window_id=window.source_window_id,
        source_series_id=window.source_series_id,
        period=window.period,
        source_artifact=window.source_artifact,
        split_kind=window.split_kind,
        condition=window.condition,
        source_start_ns=window.start_ns,
        source_end_ns=window.end_ns,
        source_row_ids=tuple(item.source_row_id for item in window.events),
        source_event_ids=tuple(item.source_event_id for item in window.events),
        event_offsets_ns=offsets,
        bid_deltas=bids,
        ask_deltas=asks,
        transitions=transitions,
        start_bid=first.bid,
        start_ask=first.ask,
        first_known_at_ns=window.first_known_at_ns,
        available_at_ns=window.available_at_ns,
        data_quality_eligible=window.data_quality_eligible,
        data_quality_reasons=window.data_quality_reasons,
        transform_policy=window.transform_policy,
        near_duplicate_signature=signature,
    )


def reference_motif_source_window_from_training_frame(
    frame: Any,
    *,
    source_artifact: ArtifactRef,
    split_kind: ReferenceMotifSplitKind,
    condition: ReferenceMotifConditionV1,
    first_known_at_ns: int,
    available_at_ns: int,
    transform_policy: ReferenceMotifTransformPolicyV1 | None = None,
) -> ReferenceMotifSourceWindowV1:
    """Project only identity/market/quality columns from an augmented frame."""
    required = {
        "series_id",
        "period",
        "row_id",
        "event_seq",
        "symbol",
        "timestamp_utc_ms",
        "bid",
        "ask",
        "training_usable",
        "training_exclusion_reason_code",
    }
    columns = set(getattr(frame, "columns", ()))
    missing = sorted(required - columns)
    if missing:
        raise ValueError(
            f"training frame lacks motif source columns: {missing}"
        )
    selected = frame.select(sorted(required)).sort(
        ["timestamp_utc_ms", "event_seq"]
    )
    rows = selected.to_dicts()
    if len(rows) < 2:
        raise ValueError(
            "training frame motif window requires at least two rows"
        )
    symbols = {_normalized_symbol(row["symbol"]) for row in rows}
    series_ids = {_required_text(row["series_id"]) for row in rows}
    periods = {_required_text(row["period"]) for row in rows}
    if (
        symbols != {condition.symbol}
        or len(series_ids) != 1
        or len(periods) != 1
    ):
        raise ValueError("training frame motif window axis is not homogeneous")
    classifications = tuple(
        (
            _strict_bool(row["training_usable"], "training_usable"),
            _nonnegative_int(
                _strict_int(
                    row["training_exclusion_reason_code"],
                    "training_exclusion_reason_code",
                ),
                "training_exclusion_reason_code",
            ),
        )
        for row in rows
    )
    if any(
        usable != (reason_code == 0) for usable, reason_code in classifications
    ):
        raise ValueError(
            "training usability and exclusion reason code disagree"
        )
    reasons = tuple(
        sorted(
            {
                str(reason_code)
                for usable, reason_code in classifications
                if not usable
            }
        )
    )
    events = tuple(
        ReferenceMotifSourceEventV1(
            event_time_ns=_strict_int(
                row["timestamp_utc_ms"], "timestamp_utc_ms"
            )
            * 1_000_000,
            event_sequence=_strict_int(row["event_seq"], "event_seq"),
            bid=_finite_float(row["bid"], "bid"),
            ask=_finite_float(row["ask"], "ask"),
            source_row_id=_strict_int(row["row_id"], "row_id"),
        )
        for row in rows
    )
    return ReferenceMotifSourceWindowV1(
        source_series_id=next(iter(series_ids)),
        period=next(iter(periods)),
        source_artifact=source_artifact,
        split_kind=split_kind,
        condition=condition,
        events=events,
        first_known_at_ns=first_known_at_ns,
        available_at_ns=available_at_ns,
        data_quality_eligible=not reasons,
        data_quality_reasons=reasons,
        transform_policy=transform_policy or ReferenceMotifTransformPolicyV1(),
    )


@dataclass(frozen=True, slots=True)
class ReferenceMotifIndexV1:
    """Bounded in-memory form of one artifact-backed empirical library."""

    config: ReferenceMotifIndexConfigV1
    splits: tuple[ReferenceMotifSplitV1, ...]
    fragments: tuple[ReferenceMotifFragmentV1, ...]
    source_window_count: int
    excluded_split_counts: Mapping[str, int]
    ineligible_window_count: int
    selection_omitted_count: int
    leakage_comparison_count: int
    lineage_artifacts: tuple[ArtifactRef, ...]
    index_id: str = ""
    schema_version: str = REFERENCE_MOTIF_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif index schema")
        if not isinstance(self.config, ReferenceMotifIndexConfigV1):
            raise ValueError("reference motif index requires a v1 config")
        splits = _validated_splits(self.splits)
        object.__setattr__(self, "splits", splits)
        fragments = tuple(
            sorted(self.fragments, key=lambda item: item.fragment_id)
        )
        if not fragments or len(fragments) > self.config.max_fragments:
            raise ValueError(
                "reference motif index fragment count is outside limits"
            )
        if any(
            not isinstance(item, ReferenceMotifFragmentV1) for item in fragments
        ):
            raise ValueError("reference motif index requires v1 fragments")
        if any(
            item.split_kind is not ReferenceMotifSplitKind.TRAIN
            or not item.data_quality_eligible
            for item in fragments
        ):
            raise ValueError(
                "reference motif index may retain only eligible train fragments"
            )
        if len({item.fragment_id for item in fragments}) != len(fragments):
            raise ValueError("reference motif index has duplicate fragment IDs")
        train = splits[0]
        if any(
            not (
                train.start_ns <= item.source_start_ns
                and item.source_end_ns < train.end_ns
            )
            for item in fragments
        ):
            raise ValueError(
                "reference motif fragment falls outside train split"
            )
        object.__setattr__(self, "fragments", fragments)
        source_count = _bounded_int(
            self.source_window_count,
            "source_window_count",
            len(fragments),
            self.config.max_source_windows,
        )
        object.__setattr__(self, "source_window_count", source_count)
        excluded = _split_count_mapping(self.excluded_split_counts)
        object.__setattr__(self, "excluded_split_counts", excluded)
        ineligible = _bounded_int(
            self.ineligible_window_count,
            "ineligible_window_count",
            0,
            source_count,
        )
        omitted = _bounded_int(
            self.selection_omitted_count,
            "selection_omitted_count",
            0,
            source_count,
        )
        comparisons = _nonnegative_int(
            self.leakage_comparison_count,
            "leakage_comparison_count",
        )
        object.__setattr__(self, "ineligible_window_count", ineligible)
        object.__setattr__(self, "selection_omitted_count", omitted)
        object.__setattr__(self, "leakage_comparison_count", comparisons)
        if (
            len(fragments) + sum(excluded.values()) + ineligible + omitted
            != source_count
        ):
            raise ValueError(
                "reference motif index source-window accounting differs"
            )
        artifacts = _unique_artifact_refs(self.lineage_artifacts)
        source_hashes = {item.source_artifact.sha256 for item in fragments}
        if not source_hashes.issubset({item.sha256 for item in artifacts}):
            raise ValueError(
                "reference motif lineage artifacts do not cover fragments"
            )
        object.__setattr__(self, "lineage_artifacts", artifacts)
        expected = _stable_id("reference-motif-index", self.identity_payload())
        supplied = _optional_text(self.index_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reference motif index_id differs")
        object.__setattr__(self, "index_id", expected)
        if len(self.to_json().encode("utf-8")) > self.config.max_artifact_bytes:
            raise ReferenceMotifResourceLimitError(
                "reference motif index exceeds configured artifact bytes"
            )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "config": self.config.to_dict(),
            "splits": [item.to_dict() for item in self.splits],
            "fragments": [item.to_dict() for item in self.fragments],
            "source_window_count": self.source_window_count,
            "excluded_split_counts": dict(self.excluded_split_counts),
            "ineligible_window_count": self.ineligible_window_count,
            "selection_omitted_count": self.selection_omitted_count,
            "leakage_comparison_count": self.leakage_comparison_count,
            "lineage_artifacts": [
                item.to_dict() for item in self.lineage_artifacts
            ],
            "retention_policy": "eligible-train-only",
            "payload_layout": "compact-offsets-and-deltas-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    def to_json(self) -> str:
        return _canonical_json(self.to_dict())

    def fragment_by_id(self, fragment_id: str) -> ReferenceMotifFragmentV1:
        selected = _required_sha256_id(
            fragment_id, "fragment_id", "reference-motif-fragment"
        )
        try:
            return next(
                item for item in self.fragments if item.fragment_id == selected
            )
        except StopIteration as err:
            raise KeyError(selected) from err

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReferenceMotifIndexV1":
        _require_schema(data, REFERENCE_MOTIF_INDEX_SCHEMA_VERSION)
        return cls(
            config=ReferenceMotifIndexConfigV1.from_dict(
                _mapping(data.get("config"))
            ),
            splits=tuple(
                ReferenceMotifSplitV1.from_dict(item)
                for item in _mapping_sequence(data.get("splits"))
            ),
            fragments=tuple(
                ReferenceMotifFragmentV1.from_dict(item)
                for item in _mapping_sequence(data.get("fragments"))
            ),
            source_window_count=_strict_int(
                data.get("source_window_count"), "source_window_count"
            ),
            excluded_split_counts={
                str(name): _strict_int(value, str(name))
                for name, value in _mapping(
                    data.get("excluded_split_counts")
                ).items()
            },
            ineligible_window_count=_strict_int(
                data.get("ineligible_window_count"), "ineligible_window_count"
            ),
            selection_omitted_count=_strict_int(
                data.get("selection_omitted_count"), "selection_omitted_count"
            ),
            leakage_comparison_count=_strict_int(
                data.get("leakage_comparison_count"), "leakage_comparison_count"
            ),
            lineage_artifacts=tuple(
                ArtifactRef.from_dict(item)
                for item in _mapping_sequence(data.get("lineage_artifacts"))
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReferenceMotifIndexV1":
        return cls.from_dict(_json_mapping(text))


def build_reference_motif_index(
    source_windows: Iterable[ReferenceMotifSourceWindowV1],
    *,
    splits: Sequence[ReferenceMotifSplitV1],
    config: ReferenceMotifIndexConfigV1 | None = None,
) -> ReferenceMotifIndexV1:
    """Build a deterministic train-only library.

    Cross-split leakage is audited before any fragment is retained.
    """
    selected = config or ReferenceMotifIndexConfigV1()
    declared_splits = _validated_splits(splits)
    values: list[ReferenceMotifSourceWindowV1] = []
    for value in source_windows:
        if not isinstance(value, ReferenceMotifSourceWindowV1):
            raise ValueError("reference motif build requires v1 source windows")
        values.append(value)
        if len(values) > selected.max_source_windows:
            raise ReferenceMotifResourceLimitError(
                "reference motif source windows exceed configured maximum"
            )
    ordered = tuple(sorted(values, key=lambda item: item.source_window_id))
    if not ordered:
        raise ValueError("reference motif index requires source windows")
    if len({item.source_window_id for item in ordered}) != len(ordered):
        raise ValueError("reference motif source_window_id is duplicated")
    by_kind = {item.kind: item for item in declared_splits}
    for window in ordered:
        split = by_kind[window.split_kind]
        if not (
            split.start_ns <= window.start_ns and window.end_ns < split.end_ns
        ):
            raise ValueError(
                "reference motif source window falls outside its split"
            )

    projected = tuple(
        extract_reference_motif_fragment(window, config=selected)
        for window in ordered
        if selected.min_events_per_fragment
        <= len(window.events)
        <= selected.max_events_per_fragment
    )
    if len(projected) != len(ordered):
        raise ReferenceMotifResourceLimitError(
            "reference motif source window event count is outside "
            "configured limits"
        )
    comparisons, findings = _cross_split_leakage_findings(
        ordered,
        projected,
        guard_ns=selected.source_overlap_guard_ns,
    )
    if findings:
        raise ReferenceMotifLeakageError(
            findings[:MAX_REFERENCE_MOTIF_LEAKAGE_FINDINGS]
        )

    excluded = Counter(
        item.split_kind.value
        for item in ordered
        if item.split_kind is not ReferenceMotifSplitKind.TRAIN
    )
    ineligible = sum(
        item.split_kind is ReferenceMotifSplitKind.TRAIN
        and not item.data_quality_eligible
        for item in ordered
    )
    candidates = [
        fragment
        for fragment in projected
        if fragment.split_kind is ReferenceMotifSplitKind.TRAIN
        and fragment.data_quality_eligible
    ]
    ranked = sorted(
        candidates,
        key=lambda item: (
            hashlib.sha256(
                f"{selected.config_id}:{item.fragment_id}".encode("utf-8")
            ).hexdigest(),
            item.fragment_id,
        ),
    )
    retained = tuple(
        sorted(
            ranked[: selected.max_fragments], key=lambda item: item.fragment_id
        )
    )
    omitted = len(candidates) - len(retained)
    if not retained:
        raise ValueError(
            "reference motif build retained no eligible train fragments"
        )
    artifacts = _unique_artifact_refs(
        tuple(item.source_artifact for item in retained)
    )
    return ReferenceMotifIndexV1(
        config=selected,
        splits=declared_splits,
        fragments=retained,
        source_window_count=len(ordered),
        excluded_split_counts=dict(excluded),
        ineligible_window_count=ineligible,
        selection_omitted_count=omitted,
        leakage_comparison_count=comparisons,
        lineage_artifacts=artifacts,
    )


def write_reference_motif_index(
    index: ReferenceMotifIndexV1,
    path: str | Path,
) -> ArtifactRef:
    """Atomically write an index and return a content-verifiable reference."""
    if not isinstance(index, ReferenceMotifIndexV1):
        raise ValueError("reference motif writer requires a v1 index")
    target = Path(path)
    payload = f"{index.to_json()}\n".encode("utf-8")
    if len(payload) > index.config.max_artifact_bytes:
        raise ReferenceMotifResourceLimitError(
            "reference motif artifact exceeds configured maximum"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(payload)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    digest = hashlib.sha256(payload).hexdigest()
    return ArtifactRef(
        kind=REFERENCE_MOTIF_ARTIFACT_KIND,
        path=str(target),
        size_bytes=len(payload),
        sha256=digest,
        metadata={
            "schema_version": index.schema_version,
            "index_id": index.index_id,
            "config_id": index.config.config_id,
            "fragment_count": len(index.fragments),
            "source_window_count": index.source_window_count,
            "payload_layout": "compact-offsets-and-deltas-v1",
        },
    )


def read_reference_motif_index(
    path: str | Path,
    *,
    artifact_ref: ArtifactRef | None = None,
) -> ReferenceMotifIndexV1:
    """Read and verify a persisted motif index and optional artifact identity."""
    target = Path(path)
    payload = target.read_bytes()
    if artifact_ref is not None:
        reference = _validated_artifact_ref(artifact_ref)
        if reference.kind != REFERENCE_MOTIF_ARTIFACT_KIND:
            raise ValueError("artifact kind is not a reference motif index")
        if reference.size_bytes != len(payload):
            raise ValueError("reference motif artifact size differs")
        if reference.sha256 != hashlib.sha256(payload).hexdigest():
            raise ValueError("reference motif artifact sha256 differs")
    return ReferenceMotifIndexV1.from_json(payload.decode("utf-8"))


@dataclass(frozen=True, slots=True)
class ReferenceMotifQueryV1:
    """One bounded deterministic retrieval request."""

    condition: ReferenceMotifConditionV1
    information_mode: InformationMode
    used_at_ns: int
    as_of_ns: int | None = None
    max_results: int = 16
    min_cell_support: int | None = None
    max_distance: float | None = None
    excluded_source_window_ids: tuple[str, ...] = ()
    query_id: str = ""
    schema_version: str = REFERENCE_MOTIF_QUERY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_QUERY_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif query schema")
        if not isinstance(self.condition, ReferenceMotifConditionV1):
            raise ValueError("reference motif query requires a v1 condition")
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        used = _bounded_int64(self.used_at_ns, "used_at_ns")
        object.__setattr__(self, "used_at_ns", used)
        if self.information_mode is InformationMode.EX_ANTE_SIMULATION:
            if self.as_of_ns is None:
                raise ValueError("ex-ante motif query requires as_of_ns")
            as_of = _bounded_int64(self.as_of_ns, "as_of_ns")
            if as_of > used:
                raise ValueError(
                    "motif query as_of_ns cannot follow used_at_ns"
                )
            object.__setattr__(self, "as_of_ns", as_of)
        elif self.as_of_ns is not None:
            raise ValueError("ex-post motif query does not accept as_of_ns")
        object.__setattr__(
            self,
            "max_results",
            _bounded_int(
                self.max_results, "max_results", 1, MAX_REFERENCE_MOTIF_MATCHES
            ),
        )
        if self.min_cell_support is not None:
            object.__setattr__(
                self,
                "min_cell_support",
                _bounded_int(
                    self.min_cell_support,
                    "min_cell_support",
                    1,
                    MAX_REFERENCE_MOTIF_FRAGMENTS,
                ),
            )
        if self.max_distance is not None:
            object.__setattr__(
                self,
                "max_distance",
                _nonnegative_float(self.max_distance, "max_distance"),
            )
        exclusions = tuple(
            sorted(
                {
                    _required_sha256_id(
                        item,
                        "excluded_source_window_id",
                        "reference-motif-source-window",
                    )
                    for item in self.excluded_source_window_ids
                }
            )
        )
        if len(exclusions) > MAX_REFERENCE_MOTIF_EXCLUSIONS:
            raise ValueError("reference motif query exclusions exceed limit")
        object.__setattr__(self, "excluded_source_window_ids", exclusions)
        expected = _stable_id("reference-motif-query", self.identity_payload())
        supplied = _optional_text(self.query_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reference motif query_id differs")
        object.__setattr__(self, "query_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "condition": self.condition.to_dict(),
            "information_mode": self.information_mode.value,
            "used_at_ns": self.used_at_ns,
            "as_of_ns": self.as_of_ns,
            "max_results": self.max_results,
            "min_cell_support": self.min_cell_support,
            "max_distance": self.max_distance,
            "excluded_source_window_ids": list(self.excluded_source_window_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "query_id": self.query_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReferenceMotifQueryV1":
        _require_schema(data, REFERENCE_MOTIF_QUERY_SCHEMA_VERSION)
        as_of = data.get("as_of_ns")
        support = data.get("min_cell_support")
        distance = data.get("max_distance")
        return cls(
            condition=ReferenceMotifConditionV1.from_dict(
                _mapping(data.get("condition"))
            ),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            used_at_ns=_strict_int(data.get("used_at_ns"), "used_at_ns"),
            as_of_ns=(
                _strict_int(as_of, "as_of_ns") if as_of is not None else None
            ),
            max_results=_strict_int(data.get("max_results"), "max_results"),
            min_cell_support=(
                _strict_int(support, "min_cell_support")
                if support is not None
                else None
            ),
            max_distance=(
                _finite_float(distance, "max_distance")
                if distance is not None
                else None
            ),
            excluded_source_window_ids=_string_tuple(
                data.get("excluded_source_window_ids")
            ),
            query_id=str(data.get("query_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReferenceMotifBackoffAttemptV1:
    """One observable fallback decision, including sparse cells."""

    level: str
    pattern: Mapping[str, str]
    candidate_count: int
    available_count: int
    minimum_support: int
    outcome: str
    schema_version: str = REFERENCE_MOTIF_BACKOFF_ATTEMPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != REFERENCE_MOTIF_BACKOFF_ATTEMPT_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reference motif backoff attempt schema"
            )
        if self.level not in REFERENCE_MOTIF_BACKOFF_LEVELS:
            raise ValueError(
                "unsupported reference motif backoff attempt level"
            )
        object.__setattr__(
            self,
            "pattern",
            {
                str(name): str(value)
                for name, value in sorted(self.pattern.items())
            },
        )
        object.__setattr__(
            self,
            "candidate_count",
            _nonnegative_int(self.candidate_count, "candidate_count"),
        )
        object.__setattr__(
            self,
            "available_count",
            _nonnegative_int(self.available_count, "available_count"),
        )
        object.__setattr__(
            self,
            "minimum_support",
            _positive_int(self.minimum_support, "minimum_support"),
        )
        outcome = _required_text(self.outcome)
        if outcome not in {
            "selected",
            "sparse",
            "unavailable",
            "distance_filtered",
        }:
            raise ValueError("unsupported reference motif backoff outcome")
        object.__setattr__(self, "outcome", outcome)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "level": self.level,
            "pattern": dict(self.pattern),
            "candidate_count": self.candidate_count,
            "available_count": self.available_count,
            "minimum_support": self.minimum_support,
            "outcome": self.outcome,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReferenceMotifBackoffAttemptV1":
        _require_schema(data, REFERENCE_MOTIF_BACKOFF_ATTEMPT_SCHEMA_VERSION)
        return cls(
            level=str(data.get("level", "")),
            pattern={
                str(name): str(value)
                for name, value in _mapping(data.get("pattern")).items()
            },
            candidate_count=_strict_int(
                data.get("candidate_count"), "candidate_count"
            ),
            available_count=_strict_int(
                data.get("available_count"), "available_count"
            ),
            minimum_support=_strict_int(
                data.get("minimum_support"), "minimum_support"
            ),
            outcome=str(data.get("outcome", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReferenceMotifMatchV1:
    """One ranked motif with transparent support and complete lineage."""

    fragment: ReferenceMotifFragmentV1
    distance: float
    cell_support: int
    backoff_level: str
    rank: int
    schema_version: str = REFERENCE_MOTIF_MATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_MATCH_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif match schema")
        if not isinstance(self.fragment, ReferenceMotifFragmentV1):
            raise ValueError("reference motif match requires a v1 fragment")
        object.__setattr__(
            self, "distance", _nonnegative_float(self.distance, "distance")
        )
        object.__setattr__(
            self,
            "cell_support",
            _positive_int(self.cell_support, "cell_support"),
        )
        if self.backoff_level not in REFERENCE_MOTIF_BACKOFF_LEVELS:
            raise ValueError("unsupported reference motif match backoff level")
        object.__setattr__(self, "rank", _positive_int(self.rank, "rank"))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "fragment": self.fragment.to_dict(),
            "distance": self.distance,
            "cell_support": self.cell_support,
            "backoff_level": self.backoff_level,
            "rank": self.rank,
            "tie_break": [self.distance, self.fragment.fragment_id],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReferenceMotifMatchV1":
        _require_schema(data, REFERENCE_MOTIF_MATCH_SCHEMA_VERSION)
        return cls(
            fragment=ReferenceMotifFragmentV1.from_dict(
                _mapping(data.get("fragment"))
            ),
            distance=_finite_float(data.get("distance"), "distance"),
            cell_support=_strict_int(data.get("cell_support"), "cell_support"),
            backoff_level=str(data.get("backoff_level", "")),
            rank=_strict_int(data.get("rank"), "rank"),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReferenceMotifQueryResultV1:
    """Bounded deterministic retrieval result and full fallback trace."""

    index_id: str
    query: ReferenceMotifQueryV1
    status: ReferenceMotifQueryStatus
    matches: tuple[ReferenceMotifMatchV1, ...]
    backoff_attempts: tuple[ReferenceMotifBackoffAttemptV1, ...]
    scanned_fragment_count: int
    hidden_by_availability_count: int
    result_id: str = ""
    schema_version: str = REFERENCE_MOTIF_QUERY_RESULT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REFERENCE_MOTIF_QUERY_RESULT_SCHEMA_VERSION:
            raise ValueError("unsupported reference motif query result schema")
        object.__setattr__(
            self,
            "index_id",
            _required_sha256_id(
                self.index_id, "index_id", "reference-motif-index"
            ),
        )
        if not isinstance(self.query, ReferenceMotifQueryV1):
            raise ValueError("reference motif result requires a v1 query")
        object.__setattr__(
            self, "status", ReferenceMotifQueryStatus(self.status)
        )
        matches = tuple(self.matches)
        attempts = tuple(self.backoff_attempts)
        if len(matches) > self.query.max_results:
            raise ValueError("reference motif result exceeds query maximum")
        if any(not isinstance(item, ReferenceMotifMatchV1) for item in matches):
            raise ValueError("reference motif result requires v1 matches")
        if any(
            not isinstance(item, ReferenceMotifBackoffAttemptV1)
            for item in attempts
        ):
            raise ValueError("reference motif result requires v1 attempts")
        if self.status is ReferenceMotifQueryStatus.MATCHED and not matches:
            raise ValueError("matched reference motif result requires matches")
        if self.status is not ReferenceMotifQueryStatus.MATCHED and matches:
            raise ValueError(
                "unmatched reference motif result cannot carry matches"
            )
        object.__setattr__(self, "matches", matches)
        object.__setattr__(self, "backoff_attempts", attempts)
        object.__setattr__(
            self,
            "scanned_fragment_count",
            _nonnegative_int(
                self.scanned_fragment_count, "scanned_fragment_count"
            ),
        )
        object.__setattr__(
            self,
            "hidden_by_availability_count",
            _nonnegative_int(
                self.hidden_by_availability_count,
                "hidden_by_availability_count",
            ),
        )
        expected = _stable_id(
            "reference-motif-query-result", self.identity_payload()
        )
        supplied = _optional_text(self.result_id)
        if supplied is not None and supplied != expected:
            raise ValueError("reference motif result_id differs")
        object.__setattr__(self, "result_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "index_id": self.index_id,
            "query": self.query.to_dict(),
            "status": self.status.value,
            "matches": [item.to_dict() for item in self.matches],
            "backoff_attempts": [
                item.to_dict() for item in self.backoff_attempts
            ],
            "scanned_fragment_count": self.scanned_fragment_count,
            "hidden_by_availability_count": self.hidden_by_availability_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "result_id": self.result_id}

    def to_json(self) -> str:
        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReferenceMotifQueryResultV1":
        _require_schema(data, REFERENCE_MOTIF_QUERY_RESULT_SCHEMA_VERSION)
        return cls(
            index_id=str(data.get("index_id", "")),
            query=ReferenceMotifQueryV1.from_dict(_mapping(data.get("query"))),
            status=ReferenceMotifQueryStatus(str(data.get("status", ""))),
            matches=tuple(
                ReferenceMotifMatchV1.from_dict(item)
                for item in _mapping_sequence(data.get("matches"))
            ),
            backoff_attempts=tuple(
                ReferenceMotifBackoffAttemptV1.from_dict(item)
                for item in _mapping_sequence(data.get("backoff_attempts"))
            ),
            scanned_fragment_count=_strict_int(
                data.get("scanned_fragment_count"), "scanned_fragment_count"
            ),
            hidden_by_availability_count=_strict_int(
                data.get("hidden_by_availability_count"),
                "hidden_by_availability_count",
            ),
            result_id=str(data.get("result_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReferenceMotifQueryResultV1":
        return cls.from_dict(_json_mapping(text))


def query_reference_motifs(
    index: ReferenceMotifIndexV1,
    query: ReferenceMotifQueryV1,
) -> ReferenceMotifQueryResultV1:
    """Retrieve the first supported fallback cell with deterministic ranking."""
    if not isinstance(index, ReferenceMotifIndexV1):
        raise ValueError("motif retrieval requires a v1 index")
    if not isinstance(query, ReferenceMotifQueryV1):
        raise ValueError("motif retrieval requires a v1 query")
    minimum = query.min_cell_support or index.config.min_cell_support
    maximum = min(query.max_results, index.config.max_matches)
    exclusions = set(query.excluded_source_window_ids)
    attempts: list[ReferenceMotifBackoffAttemptV1] = []
    hidden_ids: set[str] = set()

    for level in index.config.backoff_levels:
        pattern = query.condition.pattern_for_level(level)
        if pattern is None:
            continue
        candidates = tuple(
            item
            for item in index.fragments
            if item.source_window_id not in exclusions
            and item.condition.matches(pattern)
        )
        available: list[ReferenceMotifFragmentV1] = []
        for item in candidates:
            if (
                query.information_mode is InformationMode.EX_ANTE_SIMULATION
                and (
                    item.available_at_ns > cast(int, query.as_of_ns)
                    or item.source_end_ns > cast(int, query.as_of_ns)
                )
            ):
                hidden_ids.add(item.fragment_id)
                continue
            available.append(item)
        if len(available) < minimum:
            attempts.append(
                ReferenceMotifBackoffAttemptV1(
                    level=level,
                    pattern=pattern,
                    candidate_count=len(candidates),
                    available_count=len(available),
                    minimum_support=minimum,
                    outcome=(
                        "unavailable"
                        if candidates and not available
                        else "sparse"
                    ),
                )
            )
            continue
        ranked = sorted(
            (
                (
                    _condition_distance(
                        query.condition,
                        item.condition,
                        index.config,
                    ),
                    item,
                )
                for item in available
            ),
            key=lambda pair: (pair[0], pair[1].fragment_id),
        )
        if query.max_distance is not None:
            ranked = [pair for pair in ranked if pair[0] <= query.max_distance]
        if not ranked:
            attempts.append(
                ReferenceMotifBackoffAttemptV1(
                    level=level,
                    pattern=pattern,
                    candidate_count=len(candidates),
                    available_count=len(available),
                    minimum_support=minimum,
                    outcome="distance_filtered",
                )
            )
            continue
        matches = tuple(
            ReferenceMotifMatchV1(
                fragment=item,
                distance=distance,
                cell_support=len(available),
                backoff_level=level,
                rank=rank,
            )
            for rank, (distance, item) in enumerate(ranked[:maximum], start=1)
        )
        attempts.append(
            ReferenceMotifBackoffAttemptV1(
                level=level,
                pattern=pattern,
                candidate_count=len(candidates),
                available_count=len(available),
                minimum_support=minimum,
                outcome="selected",
            )
        )
        return ReferenceMotifQueryResultV1(
            index_id=index.index_id,
            query=query,
            status=ReferenceMotifQueryStatus.MATCHED,
            matches=matches,
            backoff_attempts=tuple(attempts),
            scanned_fragment_count=len(index.fragments) * len(attempts),
            hidden_by_availability_count=len(hidden_ids),
        )
    status = (
        ReferenceMotifQueryStatus.NOT_AVAILABLE_AS_OF
        if hidden_ids
        else ReferenceMotifQueryStatus.NO_SUPPORTED_CELL
    )
    return ReferenceMotifQueryResultV1(
        index_id=index.index_id,
        query=query,
        status=status,
        matches=(),
        backoff_attempts=tuple(attempts),
        scanned_fragment_count=len(index.fragments) * len(attempts),
        hidden_by_availability_count=len(hidden_ids),
    )


def reference_motif_information_inputs(
    result: ReferenceMotifQueryResultV1,
    *,
    run_id: str,
) -> tuple[ReconstructionInformationInputV1, ...]:
    """Bind selected motifs into #433's point-in-time leakage audit graph."""
    if not isinstance(result, ReferenceMotifQueryResultV1):
        raise ValueError("motif information binding requires a v1 result")
    run = _required_text(run_id)
    values: list[ReconstructionInformationInputV1] = []
    for match in result.matches:
        fragment = match.fragment
        lookahead = max(0, fragment.source_end_ns - result.query.used_at_ns)
        if result.query.information_mode is InformationMode.EX_ANTE_SIMULATION:
            lookahead = 0
        values.append(
            ReconstructionInformationInputV1(
                run_id=run,
                artifact_id=f"{result.index_id}:{fragment.fragment_id}",
                information_mode=result.query.information_mode,
                input_kind=InformationInputKind.EXTERNAL,
                stage=InformationStage.MOTIF_SELECTION,
                scope=InformationScope.EMPIRICAL_MOTIF,
                event_time_ns=fragment.source_start_ns,
                available_at_ns=fragment.available_at_ns,
                used_at_ns=result.query.used_at_ns,
                observation_start_ns=fragment.source_start_ns,
                observation_end_ns=fragment.source_end_ns,
                vintage_id=fragment.fragment_id,
                reason=(
                    "Empirical motif selected from "
                    f"{match.backoff_level} with distance {match.distance}."
                ),
                allowed_lookahead_ns=lookahead,
                split_kind=InformationSplitKind.TRAIN,
            )
        )
    return tuple(values)


def _validated_splits(
    splits: Sequence[ReferenceMotifSplitV1],
) -> tuple[ReferenceMotifSplitV1, ...]:
    values = tuple(splits)
    if any(not isinstance(item, ReferenceMotifSplitV1) for item in values):
        raise ValueError("reference motif splits must use the v1 contract")
    if tuple(item.kind.value for item in values) != _EXPECTED_SPLITS:
        raise ValueError(
            "reference motif splits must be train, calibration, validation, "
            "final_holdout"
        )
    for previous, current in zip(values, values[1:]):
        if previous.end_ns > current.start_ns:
            raise ValueError("reference motif splits overlap or regress")
    return values


def _cross_split_leakage_findings(
    windows: Sequence[ReferenceMotifSourceWindowV1],
    fragments: Sequence[ReferenceMotifFragmentV1],
    *,
    guard_ns: int,
) -> tuple[int, tuple[dict[str, JSONValue], ...]]:
    findings: dict[str, dict[str, JSONValue]] = {}
    comparisons = 0
    signatures: dict[str, list[ReferenceMotifFragmentV1]] = {}
    for fragment in fragments:
        signatures.setdefault(fragment.near_duplicate_signature, []).append(
            fragment
        )
    for signature, shape_group in signatures.items():
        kinds = {item.split_kind for item in shape_group}
        if len(kinds) <= 1 or ReferenceMotifSplitKind.TRAIN not in kinds:
            continue
        comparisons += len(shape_group) - 1
        payload: dict[str, JSONValue] = {
            "rule": "cross_split_near_duplicate_shape",
            "near_duplicate_signature": signature,
            "source_window_ids": _json_string_list(
                sorted(item.source_window_id for item in shape_group)
            ),
            "split_kinds": _json_string_list(
                sorted(item.split_kind.value for item in shape_group)
            ),
        }
        findings[_canonical_json(payload)] = payload

    groups: dict[tuple[str, str], list[ReferenceMotifSourceWindowV1]] = {}
    for window in windows:
        groups.setdefault(
            (window.source_artifact.sha256, window.condition.symbol), []
        ).append(window)
    for (source_hash, symbol), source_group in groups.items():
        ordered = sorted(
            source_group,
            key=lambda item: (
                item.start_ns,
                item.end_ns,
                item.source_window_id,
            ),
        )
        active: list[ReferenceMotifSourceWindowV1] = []
        for current in ordered:
            active = [
                item
                for item in active
                if item.end_ns + guard_ns >= current.start_ns
            ]
            for previous in active:
                comparisons += 1
                if previous.split_kind is current.split_kind:
                    continue
                if ReferenceMotifSplitKind.TRAIN not in {
                    previous.split_kind,
                    current.split_kind,
                }:
                    continue
                payload = {
                    "rule": "cross_split_source_window_overlap",
                    "source_artifact_sha256": source_hash,
                    "symbol": symbol,
                    "source_window_ids": _json_string_list(
                        sorted(
                            [
                                previous.source_window_id,
                                current.source_window_id,
                            ]
                        )
                    ),
                    "split_kinds": _json_string_list(
                        sorted(
                            [
                                previous.split_kind.value,
                                current.split_kind.value,
                            ]
                        )
                    ),
                    "source_overlap_guard_ns": guard_ns,
                }
                findings[_canonical_json(payload)] = payload
            active.append(current)
    return comparisons, tuple(findings[key] for key in sorted(findings))


def _near_duplicate_signature(
    *,
    symbol: str,
    offsets: Sequence[int],
    bid_deltas: Sequence[float],
    ask_deltas: Sequence[float],
    transitions: Sequence[ReferenceMotifTransition],
    digits: int,
) -> str:
    duration = max(1, offsets[-1])
    price_scale = max(
        1e-15,
        max(abs(value) for value in (*bid_deltas, *ask_deltas)),
    )
    normalized: dict[str, JSONValue] = {
        "symbol": symbol,
        "event_count": len(offsets),
        "relative_time": [round(value / duration, digits) for value in offsets],
        "relative_bid": [
            round(value / price_scale, digits) for value in bid_deltas
        ],
        "relative_ask": [
            round(value / price_scale, digits) for value in ask_deltas
        ],
        "transitions": [item.value for item in transitions],
    }
    return _stable_id("reference-motif-shape", normalized)


def _transition(
    previous: ReferenceMotifSourceEventV1,
    current: ReferenceMotifSourceEventV1,
) -> ReferenceMotifTransition:
    bid_changed = current.bid != previous.bid
    ask_changed = current.ask != previous.ask
    if bid_changed and ask_changed:
        return ReferenceMotifTransition.BOTH
    if bid_changed:
        return ReferenceMotifTransition.BID
    if ask_changed:
        return ReferenceMotifTransition.ASK
    return ReferenceMotifTransition.UNCHANGED


def _delta_transition(
    previous_bid: float,
    previous_ask: float,
    current_bid: float,
    current_ask: float,
) -> ReferenceMotifTransition:
    bid_changed = current_bid != previous_bid
    ask_changed = current_ask != previous_ask
    if bid_changed and ask_changed:
        return ReferenceMotifTransition.BOTH
    if bid_changed:
        return ReferenceMotifTransition.BID
    if ask_changed:
        return ReferenceMotifTransition.ASK
    return ReferenceMotifTransition.UNCHANGED


def _condition_distance(
    query: ReferenceMotifConditionV1,
    candidate: ReferenceMotifConditionV1,
    config: ReferenceMotifIndexConfigV1,
) -> float:
    total = 0.0
    for name, weight in config.metric_weights.items():
        left = query.metrics.get(name)
        right = candidate.metrics.get(name)
        if left is None or right is None:
            if left is not right:
                total += weight * config.missing_metric_penalty
            continue
        total += weight * abs(left - right) / config.metric_scales[name]
    left_coordinates = query.coordinates()
    right_coordinates = candidate.coordinates()
    mismatches = sum(
        left_coordinates[name] != right_coordinates[name]
        for name in left_coordinates
    )
    total += mismatches * config.categorical_mismatch_penalty
    return round(total, config.rounding_digits)


def _metric_mapping(value: Mapping[str, float]) -> dict[str, float]:
    result: dict[str, float] = {}
    for raw_name, raw_value in sorted(value.items()):
        name = str(raw_name)
        if name not in REFERENCE_MOTIF_METRIC_NAMES:
            raise ValueError(f"unsupported reference motif metric: {name}")
        metric = _finite_float(raw_value, name)
        if name in _NONNEGATIVE_METRICS and metric < 0:
            raise ValueError(
                f"reference motif metric {name} must be non-negative"
            )
        result[name] = metric
    return result


def _named_positive_mapping(
    value: Mapping[str, float], name: str
) -> dict[str, float]:
    keys = set(value)
    if keys != set(REFERENCE_MOTIF_METRIC_NAMES):
        raise ValueError(f"{name} must cover every reference motif metric")
    return {
        metric: _positive_float(value[metric], f"{name}.{metric}")
        for metric in REFERENCE_MOTIF_METRIC_NAMES
    }


def _named_nonnegative_mapping(
    value: Mapping[str, float], name: str
) -> dict[str, float]:
    keys = set(value)
    if keys != set(REFERENCE_MOTIF_METRIC_NAMES):
        raise ValueError(f"{name} must cover every reference motif metric")
    return {
        metric: _nonnegative_float(value[metric], f"{name}.{metric}")
        for metric in REFERENCE_MOTIF_METRIC_NAMES
    }


def _validated_artifact_ref(value: ArtifactRef) -> ArtifactRef:
    if not isinstance(value, ArtifactRef):
        raise ValueError("reference motif artifact must be an ArtifactRef")
    kind = _required_text(value.kind)
    path = _required_text(value.path)
    digest = _required_sha256(value.sha256, "artifact sha256")
    size = value.size_bytes
    if size is not None and (_strict_int(size, "artifact size_bytes") < 0):
        raise ValueError("reference motif artifact size must be non-negative")
    metadata = dict(value.metadata)
    _validate_json_value(metadata, "artifact.metadata")
    return ArtifactRef(
        kind=kind,
        path=path,
        size_bytes=size,
        sha256=digest,
        metadata=metadata,
    )


def _unique_artifact_refs(
    values: Sequence[ArtifactRef],
) -> tuple[ArtifactRef, ...]:
    by_identity: dict[tuple[str, str, str], ArtifactRef] = {}
    for raw in values:
        value = _validated_artifact_ref(raw)
        key = (value.kind, value.path, value.sha256)
        by_identity[key] = value
    return tuple(by_identity[key] for key in sorted(by_identity))


def _split_count_mapping(value: Mapping[str, int]) -> dict[str, int]:
    allowed = {item.value for item in ReferenceMotifSplitKind} - {
        ReferenceMotifSplitKind.TRAIN.value
    }
    result: dict[str, int] = {}
    for name, count in sorted(value.items()):
        if name not in allowed:
            raise ValueError("unsupported excluded reference motif split")
        normalized = _nonnegative_int(count, f"excluded_split_counts.{name}")
        if normalized:
            result[name] = normalized
    return result


def _normalized_symbol(value: Any) -> str:
    symbol = _required_text(value).upper()
    if not re.fullmatch(r"[A-Z0-9]{3,16}", symbol):
        raise ValueError("invalid reference motif symbol")
    return symbol


def _symbol_currencies(symbol: str) -> tuple[str, ...]:
    if len(symbol) == 6 and symbol.isalpha():
        return (symbol[:3], symbol[3:])
    return ()


def _normalized_currencies(values: Iterable[Any]) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item).upper() for item in values}))
    if not result or len(result) > MAX_REFERENCE_MOTIF_TAGS:
        raise ValueError("reference motif currencies are outside limits")
    if any(not re.fullmatch(r"[A-Z]{3}", item) for item in result):
        raise ValueError("reference motif currencies require ISO-like codes")
    return result


def _normalized_text_tuple(
    values: Iterable[Any], *, maximum: int
) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item) for item in values}))
    if len(result) > maximum:
        raise ValueError("reference motif text tuple exceeds limit")
    return result


def _required_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("reference motif text is required")
    if len(text) > MAX_REFERENCE_MOTIF_TEXT:
        raise ValueError("reference motif text exceeds limit")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return _required_text(text) if text else None


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a bool")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _bounded_int64(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if not INT64_MIN <= result <= INT64_MAX:
        raise ValueError(f"{name} exceeds int64")
    return result


def _nonnegative_int64(value: Any, name: str) -> int:
    result = _bounded_int64(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _positive_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    result = _strict_int(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} is outside configured bounds")
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


def _positive_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _nonnegative_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _required_sha256(value: Any, name: str) -> str:
    text = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(text):
        raise ValueError(f"{name} must be a lowercase sha256")
    return text


def _required_sha256_id(value: Any, name: str, prefix: str) -> str:
    text = _required_text(value)
    marker = f"{prefix}:sha256:"
    if not text.startswith(marker) or not _SHA256_RE.fullmatch(
        text[len(marker) :]
    ):
        raise ValueError(f"{name} must be a {prefix} sha256 ID")
    return text


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        _canonical_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if str(data.get("schema_version", "")) != expected:
        raise ValueError("unsupported reference motif schema version")


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("reference motif value must be a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise ValueError("reference motif value must be a sequence")
    return value


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(_mapping(item) for item in _sequence(value))


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _json_string_list(values: Iterable[str]) -> list[JSONValue]:
    return [value for value in values]


def _int_tuple(value: Any) -> tuple[int, ...]:
    return tuple(
        _strict_int(item, "sequence integer") for item in _sequence(value)
    )


def _float_tuple(value: Any) -> tuple[float, ...]:
    return tuple(
        _finite_float(item, "sequence float") for item in _sequence(value)
    )


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        return _mapping(json.loads(text))
    except json.JSONDecodeError as err:
        raise ValueError("invalid reference motif JSON") from err


def _validate_json_value(value: Any, path: str) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite float")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} contains a non-string key")
            _validate_json_value(item, f"{path}.{key}")
        return
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    raise ValueError(f"{path} contains a non-JSON value")


__all__ = [
    "MAX_REFERENCE_MOTIF_ARTIFACT_BYTES",
    "MAX_REFERENCE_MOTIF_EVENTS",
    "MAX_REFERENCE_MOTIF_FRAGMENTS",
    "MAX_REFERENCE_MOTIF_MATCHES",
    "MAX_REFERENCE_MOTIF_SOURCE_WINDOWS",
    "REFERENCE_MOTIF_ARTIFACT_KIND",
    "REFERENCE_MOTIF_BACKOFF_ATTEMPT_SCHEMA_VERSION",
    "REFERENCE_MOTIF_BACKOFF_LEVELS",
    "REFERENCE_MOTIF_CONDITION_SCHEMA_VERSION",
    "REFERENCE_MOTIF_FRAGMENT_SCHEMA_VERSION",
    "REFERENCE_MOTIF_INDEX_CONFIG_SCHEMA_VERSION",
    "REFERENCE_MOTIF_INDEX_SCHEMA_VERSION",
    "REFERENCE_MOTIF_MATCH_SCHEMA_VERSION",
    "REFERENCE_MOTIF_METRIC_NAMES",
    "REFERENCE_MOTIF_QUERY_RESULT_SCHEMA_VERSION",
    "REFERENCE_MOTIF_QUERY_SCHEMA_VERSION",
    "REFERENCE_MOTIF_SOURCE_EVENT_SCHEMA_VERSION",
    "REFERENCE_MOTIF_SOURCE_WINDOW_SCHEMA_VERSION",
    "REFERENCE_MOTIF_SPLIT_SCHEMA_VERSION",
    "REFERENCE_MOTIF_TRANSFORM_SCHEMA_VERSION",
    "ReferenceMotifBackoffAttemptV1",
    "ReferenceMotifConditionV1",
    "ReferenceMotifFragmentV1",
    "ReferenceMotifIndexConfigV1",
    "ReferenceMotifIndexV1",
    "ReferenceMotifLeakageError",
    "ReferenceMotifMatchV1",
    "ReferenceMotifQueryResultV1",
    "ReferenceMotifQueryStatus",
    "ReferenceMotifQueryV1",
    "ReferenceMotifResourceLimitError",
    "ReferenceMotifSourceEventV1",
    "ReferenceMotifSourceWindowV1",
    "ReferenceMotifSplitKind",
    "ReferenceMotifSplitV1",
    "ReferenceMotifTransformPolicyV1",
    "ReferenceMotifTransition",
    "build_reference_motif_index",
    "extract_reference_motif_fragment",
    "query_reference_motifs",
    "read_reference_motif_index",
    "reference_motif_information_inputs",
    "reference_motif_source_window_from_training_frame",
    "write_reference_motif_index",
]
