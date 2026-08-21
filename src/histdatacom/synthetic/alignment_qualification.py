"""Qualification of exact and bounded-prior triangle alignment.

The production planner is deliberately compact.  This module supplies the
larger validation-only evidence surface needed to decide whether its bounded
nearest-prior policy is scientifically admissible.  It enumerates every probe
leg, content-binds every selected source event, publishes quote-age slices,
compares exact and bounded outputs, and fails closed when runtime consumption
or age-conditioned output coherence differs from the frozen policy.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from bisect import bisect_right
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from itertools import pairwise
from pathlib import Path
from typing import Any

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

TRIANGLE_ALIGNMENT_SOURCE_EVENT_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-source-event.v1"
)
TRIANGLE_ALIGNMENT_SOURCE_WINDOW_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-source-window.v1"
)
TRIANGLE_ALIGNMENT_TUPLE_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-tuple.v1"
)
TRIANGLE_ALIGNMENT_WINDOW_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-window-evidence.v1"
)
TRIANGLE_SUPPORT_CENSUS_SCHEMA_VERSION = (
    "histdatacom.triangle-support-census.v1"
)
TRIANGLE_QUOTE_AGE_SLICE_SCHEMA_VERSION = (
    "histdatacom.triangle-quote-age-slice.v1"
)
TRIANGLE_ALIGNMENT_METRIC_TOLERANCE_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-metric-tolerance.v1"
)
TRIANGLE_ALIGNMENT_OUTCOME_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-outcome.v1"
)
TRIANGLE_ALIGNMENT_COMPARISON_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-comparison.v1"
)
TRIANGLE_ALIGNMENT_AGE_RULE_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-age-rule.v1"
)
TRIANGLE_ALIGNMENT_RESIDUAL_BIN_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-residual-bin.v1"
)
TRIANGLE_ALIGNMENT_CONSUMPTION_RECEIPT_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-consumption-receipt.v1"
)
TRIANGLE_ALIGNMENT_QUALIFICATION_POLICY_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-qualification-policy.v1"
)
TRIANGLE_ALIGNMENT_QUALIFICATION_SCHEMA_VERSION = (
    "histdatacom.triangle-alignment-qualification.v1"
)

TRIANGLE_ALIGNMENT_QUALIFICATION_ARTIFACT_KIND = (
    "triangle_alignment_qualification_v1"
)
TRIANGLE_SYMBOLS = ("eurgbp", "eurusd", "gbpusd")
REQUIRED_ALIGNMENT_METRICS = frozenset(
    {
        "downstream_sensitivity",
        "mark_transition_distance",
        "path_variation",
        "projection_burden",
        "synthetic_count_total",
        "triangle_residual",
    }
)
REQUIRED_AGE_SLICE_DIMENSIONS = (
    "symbol_probe_leg",
    "year",
    "feed_epoch",
    "session",
    "event_state",
    "activity_stratum",
)

MAX_ALIGNMENT_EVENTS_PER_WINDOW = 250_000
MAX_ALIGNMENT_TUPLES_PER_WINDOW = 250_000
MAX_ALIGNMENT_WINDOWS = 16_384
MAX_ALIGNMENT_OUTCOMES = 262_144
MAX_ALIGNMENT_RESIDUAL_BINS = 16_384
MAX_ALIGNMENT_ARTIFACT_BYTES = 128 * 1024 * 1024
MAX_ALIGNMENT_TEXT = 512
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class TriangleSourceWindowState(str, Enum):
    """Observed source state before any alignment decision."""

    AVAILABLE = "available"
    INCOMPLETE = "incomplete"
    EMPTY = "empty"
    EXPECTED_CLOSURE = "expected_closure"


class TriangleAlignmentPolicy(str, Enum):
    """The only two executable triangle alignment treatments."""

    EXACT_EVENT_SEQUENCE = "exact_event_sequence"
    BOUNDED_PRIOR = "bounded_prior"


class TriangleSupportClass(str, Enum):
    """Mutually exclusive source-window support classification."""

    EXACT = "exact"
    BOUNDED_PRIOR_ONLY = "bounded_prior_only"
    UNSUPPORTED_COMPLETE = "unsupported_complete"
    INCOMPLETE_SOURCE = "incomplete_source"
    EMPTY = "empty"
    EXPECTED_CLOSURE = "expected_closure"


class TriangleToleranceSeverity(str, Enum):
    """Whether a sensitivity breach blocks release."""

    HARD = "hard"
    ADVISORY = "advisory"


class TriangleAgeRuleAction(str, Enum):
    """Whether an age region is qualified or terminally refused."""

    ADMIT = "admit"
    REFUSE = "refuse"


class TriangleQualificationStatus(str, Enum):
    """Fail-closed qualification status."""

    PASS = "pass"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class TriangleAlignmentSourceEventV1:
    """One immutable, quote-bearing source row used by qualification."""

    source_event_id: str
    symbol: str
    event_time_ns: int
    event_sequence: int
    bid: float
    ask: float
    source_partition_id: str
    source_row_content_sha256: str
    event_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_SOURCE_EVENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, TRIANGLE_ALIGNMENT_SOURCE_EVENT_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "source_event_id", _required_text(self.source_event_id)
        )
        symbol = _symbol(self.symbol)
        if symbol not in TRIANGLE_SYMBOLS:
            raise ValueError("alignment source event is outside the triangle")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(
            self, "event_time_ns", _int64(self.event_time_ns, "event_time_ns")
        )
        object.__setattr__(
            self,
            "event_sequence",
            _nonnegative_int(self.event_sequence, "event_sequence"),
        )
        bid = _positive_finite(self.bid, "bid")
        ask = _positive_finite(self.ask, "ask")
        if ask < bid:
            raise ValueError("alignment source quote is crossed")
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        object.__setattr__(
            self,
            "source_partition_id",
            _required_text(self.source_partition_id),
        )
        object.__setattr__(
            self,
            "source_row_content_sha256",
            _sha256(
                self.source_row_content_sha256, "source_row_content_sha256"
            ),
        )
        expected = _stable_id("triangle-alignment-source-event", self.payload())
        if self.event_id and self.event_id != expected:
            raise ValueError("alignment source event identity differs")
        object.__setattr__(self, "event_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_event_id": self.source_event_id,
            "symbol": self.symbol,
            "event_time_ns": self.event_time_ns,
            "event_sequence": self.event_sequence,
            "bid": self.bid,
            "ask": self.ask,
            "source_partition_id": self.source_partition_id,
            "source_row_content_sha256": self.source_row_content_sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "event_id": self.event_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> TriangleAlignmentSourceEventV1:
        return cls(
            source_event_id=str(data.get("source_event_id", "")),
            symbol=str(data.get("symbol", "")),
            event_time_ns=_strict_int(
                data.get("event_time_ns"), "event_time_ns"
            ),
            event_sequence=_strict_int(
                data.get("event_sequence"), "event_sequence"
            ),
            bid=_finite_float(data.get("bid"), "bid"),
            ask=_finite_float(data.get("ask"), "ask"),
            source_partition_id=str(data.get("source_partition_id", "")),
            source_row_content_sha256=str(
                data.get("source_row_content_sha256", "")
            ),
            event_id=str(data.get("event_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleAlignmentSourceWindowV1:
    """A stratified half-open source window covering the candidate range."""

    candidate_id: str
    start_ns: int
    end_ns: int
    year: int
    feed_epoch: str
    session: str
    event_state: str
    activity_stratum: str
    source_state: TriangleSourceWindowState
    events: tuple[TriangleAlignmentSourceEventV1, ...]
    symbols: tuple[str, ...] = TRIANGLE_SYMBOLS
    source_content_sha256: str = ""
    window_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_SOURCE_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, TRIANGLE_ALIGNMENT_SOURCE_WINDOW_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "candidate_id", _required_text(self.candidate_id)
        )
        start = _int64(self.start_ns, "start_ns")
        end = _int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("alignment source window is empty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        year = _strict_int(self.year, "year")
        if not 1970 <= year <= 9999:
            raise ValueError("alignment source year is invalid")
        object.__setattr__(self, "year", year)
        for name in (
            "feed_epoch",
            "session",
            "event_state",
            "activity_stratum",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        symbols = tuple(sorted({_symbol(item) for item in self.symbols}))
        if symbols != TRIANGLE_SYMBOLS:
            raise ValueError("alignment source window requires exact triangle")
        object.__setattr__(self, "symbols", symbols)
        events = tuple(sorted(self.events, key=_event_order_key))
        if len(events) > MAX_ALIGNMENT_EVENTS_PER_WINDOW:
            raise ValueError("alignment source window exceeds event bound")
        if len({item.event_id for item in events}) != len(events):
            raise ValueError("alignment source window repeats event identity")
        if any(
            item.symbol not in symbols or not start <= item.event_time_ns < end
            for item in events
        ):
            raise ValueError("alignment source event lies outside its window")
        object.__setattr__(self, "events", events)
        state = TriangleSourceWindowState(self.source_state)
        present = {item.symbol for item in events}
        if state is TriangleSourceWindowState.AVAILABLE and present != set(
            symbols
        ):
            raise ValueError("available alignment window lacks a triangle leg")
        if state is TriangleSourceWindowState.INCOMPLETE and (
            not events or present == set(symbols)
        ):
            raise ValueError("incomplete alignment window state differs")
        if (
            state
            in {
                TriangleSourceWindowState.EMPTY,
                TriangleSourceWindowState.EXPECTED_CLOSURE,
            }
            and events
        ):
            raise ValueError("empty/closure alignment window contains events")
        object.__setattr__(self, "source_state", state)
        event_digest = _content_sha256([item.to_dict() for item in events])
        if (
            self.source_content_sha256
            and self.source_content_sha256 != event_digest
        ):
            raise ValueError("alignment source window content hash differs")
        object.__setattr__(self, "source_content_sha256", event_digest)
        expected = _stable_id(
            "triangle-alignment-source-window", self.payload()
        )
        if self.window_id and self.window_id != expected:
            raise ValueError("alignment source window identity differs")
        object.__setattr__(self, "window_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "year": self.year,
            "feed_epoch": self.feed_epoch,
            "session": self.session,
            "event_state": self.event_state,
            "activity_stratum": self.activity_stratum,
            "source_state": self.source_state.value,
            "symbols": list(self.symbols),
            "events": [item.to_dict() for item in self.events],
            "source_content_sha256": self.source_content_sha256,
            "half_open_ownership": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "window_id": self.window_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> TriangleAlignmentSourceWindowV1:
        if data.get("half_open_ownership") is not True:
            raise ValueError("alignment source ownership policy differs")
        return cls(
            candidate_id=str(data.get("candidate_id", "")),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            year=_strict_int(data.get("year"), "year"),
            feed_epoch=str(data.get("feed_epoch", "")),
            session=str(data.get("session", "")),
            event_state=str(data.get("event_state", "")),
            activity_stratum=str(data.get("activity_stratum", "")),
            source_state=TriangleSourceWindowState(
                str(data.get("source_state", ""))
            ),
            events=tuple(
                TriangleAlignmentSourceEventV1.from_dict(_mapping(item))
                for item in _sequence(data.get("events"))
            ),
            symbols=_string_tuple(data.get("symbols")),
            source_content_sha256=str(data.get("source_content_sha256", "")),
            window_id=str(data.get("window_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleAlignmentTupleV1:
    """One identity-aware exact or bounded-prior triangle tuple."""

    window_id: str
    policy: TriangleAlignmentPolicy
    configured_max_age_ns: int
    probe_symbol: str
    probe_event_id: str
    probe_time_ns: int
    selected_event_ids: Mapping[str, str]
    selected_event_times_ns: Mapping[str, int]
    selected_event_content_sha256: Mapping[str, str]
    ages_ns: Mapping[str, int]
    tuple_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_TUPLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, TRIANGLE_ALIGNMENT_TUPLE_SCHEMA_VERSION
        )
        object.__setattr__(self, "window_id", _required_text(self.window_id))
        policy = TriangleAlignmentPolicy(self.policy)
        object.__setattr__(self, "policy", policy)
        maximum_age = _nonnegative_int(
            self.configured_max_age_ns, "configured_max_age_ns"
        )
        if (
            policy is TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE
            and maximum_age
        ):
            raise ValueError("exact tuple cannot declare bounded age")
        if policy is TriangleAlignmentPolicy.BOUNDED_PRIOR and not maximum_age:
            raise ValueError(
                "bounded-prior tuple requires a positive age bound"
            )
        object.__setattr__(self, "configured_max_age_ns", maximum_age)
        probe_symbol = _symbol(self.probe_symbol)
        if probe_symbol not in TRIANGLE_SYMBOLS:
            raise ValueError("alignment tuple probe is outside triangle")
        object.__setattr__(self, "probe_symbol", probe_symbol)
        object.__setattr__(
            self, "probe_event_id", _required_text(self.probe_event_id)
        )
        probe_time = _int64(self.probe_time_ns, "probe_time_ns")
        object.__setattr__(self, "probe_time_ns", probe_time)
        ids = _triangle_text_mapping(
            self.selected_event_ids, "selected_event_ids"
        )
        times = _triangle_int_mapping(
            self.selected_event_times_ns, "selected_event_times_ns"
        )
        hashes = _triangle_sha_mapping(
            self.selected_event_content_sha256,
            "selected_event_content_sha256",
        )
        ages = _triangle_int_mapping(self.ages_ns, "ages_ns")
        if ids[probe_symbol] != self.probe_event_id:
            raise ValueError("alignment tuple probe event is not selected")
        if times[probe_symbol] != probe_time or ages[probe_symbol] != 0:
            raise ValueError("alignment tuple probe identity was retimestamped")
        for symbol in TRIANGLE_SYMBOLS:
            age = ages[symbol]
            if age < 0 or probe_time - times[symbol] != age:
                raise ValueError(
                    "alignment tuple uses a future or retimestamped event"
                )
            if age > maximum_age:
                raise ValueError(
                    "alignment tuple silently widens the age bound"
                )
        if policy is TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE and any(
            ages.values()
        ):
            raise ValueError("exact tuple contains a stale event")
        object.__setattr__(self, "selected_event_ids", ids)
        object.__setattr__(self, "selected_event_times_ns", times)
        object.__setattr__(self, "selected_event_content_sha256", hashes)
        object.__setattr__(self, "ages_ns", ages)
        expected = _stable_id("triangle-alignment-tuple", self.payload())
        if self.tuple_id and self.tuple_id != expected:
            raise ValueError("alignment tuple identity differs")
        object.__setattr__(self, "tuple_id", expected)

    @property
    def maximum_age_ns(self) -> int:
        return max(self.ages_ns.values(), default=0)

    @property
    def bounded_only(self) -> bool:
        return self.maximum_age_ns > 0

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "window_id": self.window_id,
            "policy": self.policy.value,
            "configured_max_age_ns": self.configured_max_age_ns,
            "probe_symbol": self.probe_symbol,
            "probe_event_id": self.probe_event_id,
            "probe_time_ns": self.probe_time_ns,
            "selected_event_ids": dict(self.selected_event_ids),
            "selected_event_times_ns": dict(self.selected_event_times_ns),
            "selected_event_content_sha256": dict(
                self.selected_event_content_sha256
            ),
            "ages_ns": dict(self.ages_ns),
            "future_event_allowed": False,
            "retimestamping_allowed": False,
            "interpolation_allowed": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "tuple_id": self.tuple_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TriangleAlignmentTupleV1:
        for key in (
            "future_event_allowed",
            "retimestamping_allowed",
            "interpolation_allowed",
        ):
            if data.get(key) is not False:
                raise ValueError("alignment tuple safety policy differs")
        return cls(
            window_id=str(data.get("window_id", "")),
            policy=TriangleAlignmentPolicy(str(data.get("policy", ""))),
            configured_max_age_ns=_strict_int(
                data.get("configured_max_age_ns"), "configured_max_age_ns"
            ),
            probe_symbol=str(data.get("probe_symbol", "")),
            probe_event_id=str(data.get("probe_event_id", "")),
            probe_time_ns=_strict_int(
                data.get("probe_time_ns"), "probe_time_ns"
            ),
            selected_event_ids=_text_mapping(data.get("selected_event_ids")),
            selected_event_times_ns=_int_mapping(
                data.get("selected_event_times_ns")
            ),
            selected_event_content_sha256=_text_mapping(
                data.get("selected_event_content_sha256")
            ),
            ages_ns=_int_mapping(data.get("ages_ns")),
            tuple_id=str(data.get("tuple_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleAlignmentWindowEvidenceV1:
    """Complete support decision and all selected tuples for one window."""

    candidate_id: str
    source_window_id: str
    start_ns: int
    end_ns: int
    year: int
    feed_epoch: str
    session: str
    event_state: str
    activity_stratum: str
    source_state: TriangleSourceWindowState
    support_class: TriangleSupportClass
    source_event_counts: Mapping[str, int]
    exact_event_sequence_support: int
    bounded_support_by_probe_leg: Mapping[str, int]
    bounded_only_support_by_probe_leg: Mapping[str, int]
    selected_policy: TriangleAlignmentPolicy | None
    selected_probe_leg: str | None
    configured_max_age_ns: int
    selected_tuples: tuple[TriangleAlignmentTupleV1, ...]
    source_content_sha256: str
    recommended_event_time_ns: int | None
    evidence_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_WINDOW_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            TRIANGLE_ALIGNMENT_WINDOW_EVIDENCE_SCHEMA_VERSION,
        )
        for name in ("candidate_id", "source_window_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        start = _int64(self.start_ns, "start_ns")
        end = _int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("alignment evidence window is empty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        year = _strict_int(self.year, "year")
        if not 1970 <= year <= 9999:
            raise ValueError("alignment evidence year is invalid")
        object.__setattr__(self, "year", year)
        for name in (
            "feed_epoch",
            "session",
            "event_state",
            "activity_stratum",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        state = TriangleSourceWindowState(self.source_state)
        support = TriangleSupportClass(self.support_class)
        object.__setattr__(self, "source_state", state)
        object.__setattr__(self, "support_class", support)
        counts = _triangle_int_mapping(
            self.source_event_counts, "source_event_counts"
        )
        if any(value < 0 for value in counts.values()):
            raise ValueError("alignment source event count is negative")
        object.__setattr__(self, "source_event_counts", counts)
        exact = _nonnegative_int(
            self.exact_event_sequence_support,
            "exact_event_sequence_support",
        )
        object.__setattr__(self, "exact_event_sequence_support", exact)
        bounded = _triangle_int_mapping(
            self.bounded_support_by_probe_leg,
            "bounded_support_by_probe_leg",
        )
        bounded_only = _triangle_int_mapping(
            self.bounded_only_support_by_probe_leg,
            "bounded_only_support_by_probe_leg",
        )
        if any(
            bounded[symbol] < 0
            or bounded_only[symbol] < 0
            or bounded_only[symbol] > bounded[symbol]
            for symbol in TRIANGLE_SYMBOLS
        ):
            raise ValueError("bounded alignment probe counts are invalid")
        object.__setattr__(self, "bounded_support_by_probe_leg", bounded)
        object.__setattr__(
            self, "bounded_only_support_by_probe_leg", bounded_only
        )
        policy = (
            TriangleAlignmentPolicy(self.selected_policy)
            if self.selected_policy is not None
            else None
        )
        probe = (
            _symbol(self.selected_probe_leg)
            if self.selected_probe_leg
            else None
        )
        maximum_age = _nonnegative_int(
            self.configured_max_age_ns, "configured_max_age_ns"
        )
        tuples = tuple(self.selected_tuples)
        if len(tuples) > MAX_ALIGNMENT_TUPLES_PER_WINDOW:
            raise ValueError("alignment evidence exceeds tuple bound")
        if len({item.tuple_id for item in tuples}) != len(tuples):
            raise ValueError("alignment evidence repeats tuple identity")
        if any(item.window_id != self.source_window_id for item in tuples):
            raise ValueError("alignment tuple belongs to another source window")
        if policy is None:
            if (
                probe is not None
                or tuples
                or self.recommended_event_time_ns is not None
            ):
                raise ValueError(
                    "unsupported alignment cannot retain a selection"
                )
        else:
            if probe not in TRIANGLE_SYMBOLS or not tuples:
                raise ValueError("supported alignment lacks a selected probe")
            if any(
                item.policy is not policy
                or item.probe_symbol != probe
                or item.configured_max_age_ns != maximum_age
                for item in tuples
            ):
                raise ValueError("selected alignment tuples differ from policy")
            if policy is TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE:
                if maximum_age or len(tuples) != exact:
                    raise ValueError("exact alignment evidence count differs")
            elif len(tuples) != bounded[probe]:
                raise ValueError("bounded alignment evidence count differs")
        recommended = self.recommended_event_time_ns
        if recommended is not None:
            recommended = _int64(recommended, "recommended_event_time_ns")
            if not start <= recommended < end or recommended not in {
                item.probe_time_ns for item in tuples
            }:
                raise ValueError("recommended alignment event is unsupported")
        object.__setattr__(self, "selected_policy", policy)
        object.__setattr__(self, "selected_probe_leg", probe)
        object.__setattr__(self, "configured_max_age_ns", maximum_age)
        object.__setattr__(self, "selected_tuples", tuples)
        object.__setattr__(self, "recommended_event_time_ns", recommended)
        object.__setattr__(
            self,
            "source_content_sha256",
            _sha256(self.source_content_sha256, "source_content_sha256"),
        )
        _validate_support_class(self)
        expected = _stable_id(
            "triangle-alignment-window-evidence", self.payload()
        )
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError("alignment window evidence identity differs")
        object.__setattr__(self, "evidence_id", expected)

    @property
    def selected_tuple_content_sha256(self) -> str:
        return _content_sha256(
            [item.to_dict() for item in self.selected_tuples]
        )

    @property
    def selected_event_content_sha256(self) -> str:
        return _content_sha256(
            [
                {
                    "tuple_id": item.tuple_id,
                    "event_ids": dict(item.selected_event_ids),
                    "event_hashes": dict(item.selected_event_content_sha256),
                }
                for item in self.selected_tuples
            ]
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "source_window_id": self.source_window_id,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "year": self.year,
            "feed_epoch": self.feed_epoch,
            "session": self.session,
            "event_state": self.event_state,
            "activity_stratum": self.activity_stratum,
            "source_state": self.source_state.value,
            "support_class": self.support_class.value,
            "source_event_counts": dict(self.source_event_counts),
            "exact_event_sequence_support": self.exact_event_sequence_support,
            "bounded_support_by_probe_leg": dict(
                self.bounded_support_by_probe_leg
            ),
            "bounded_only_support_by_probe_leg": dict(
                self.bounded_only_support_by_probe_leg
            ),
            "selected_policy": (
                self.selected_policy.value if self.selected_policy else None
            ),
            "selected_probe_leg": self.selected_probe_leg,
            "configured_max_age_ns": self.configured_max_age_ns,
            "selected_tuples": [
                item.to_dict() for item in self.selected_tuples
            ],
            "selected_tuple_content_sha256": self.selected_tuple_content_sha256,
            "selected_event_content_sha256": self.selected_event_content_sha256,
            "source_content_sha256": self.source_content_sha256,
            "recommended_event_time_ns": self.recommended_event_time_ns,
            "all_probe_legs_reported": True,
            "observed_source_mutable": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> TriangleAlignmentWindowEvidenceV1:
        if data.get("all_probe_legs_reported") is not True:
            raise ValueError("alignment alternative-probe policy differs")
        if data.get("observed_source_mutable") is not False:
            raise ValueError("alignment observed-source policy differs")
        result = cls(
            candidate_id=str(data.get("candidate_id", "")),
            source_window_id=str(data.get("source_window_id", "")),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            year=_strict_int(data.get("year"), "year"),
            feed_epoch=str(data.get("feed_epoch", "")),
            session=str(data.get("session", "")),
            event_state=str(data.get("event_state", "")),
            activity_stratum=str(data.get("activity_stratum", "")),
            source_state=TriangleSourceWindowState(
                str(data.get("source_state", ""))
            ),
            support_class=TriangleSupportClass(
                str(data.get("support_class", ""))
            ),
            source_event_counts=_int_mapping(data.get("source_event_counts")),
            exact_event_sequence_support=_strict_int(
                data.get("exact_event_sequence_support"),
                "exact_event_sequence_support",
            ),
            bounded_support_by_probe_leg=_int_mapping(
                data.get("bounded_support_by_probe_leg")
            ),
            bounded_only_support_by_probe_leg=_int_mapping(
                data.get("bounded_only_support_by_probe_leg")
            ),
            selected_policy=(
                TriangleAlignmentPolicy(str(data.get("selected_policy")))
                if data.get("selected_policy") is not None
                else None
            ),
            selected_probe_leg=(
                str(data.get("selected_probe_leg"))
                if data.get("selected_probe_leg") is not None
                else None
            ),
            configured_max_age_ns=_strict_int(
                data.get("configured_max_age_ns"), "configured_max_age_ns"
            ),
            selected_tuples=tuple(
                TriangleAlignmentTupleV1.from_dict(_mapping(item))
                for item in _sequence(data.get("selected_tuples"))
            ),
            source_content_sha256=str(data.get("source_content_sha256", "")),
            recommended_event_time_ns=(
                _strict_int(
                    data.get("recommended_event_time_ns"),
                    "recommended_event_time_ns",
                )
                if data.get("recommended_event_time_ns") is not None
                else None
            ),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("selected_tuple_content_sha256") != (
            result.selected_tuple_content_sha256
        ) or data.get("selected_event_content_sha256") != (
            result.selected_event_content_sha256
        ):
            raise ValueError("alignment selected-event digest differs")
        return result


@dataclass(frozen=True, slots=True)
class TriangleSupportCensusV1:
    """Complete-range support decomposition and bounded coverage fractions."""

    candidate_id: str
    start_ns: int
    end_ns: int
    window_counts: Mapping[str, int]
    duration_ns_by_support_class: Mapping[str, int]
    exact_event_sequence_support: int
    bounded_prior_support: int
    bounded_prior_only_support: int
    selected_probe_leg_counts: Mapping[str, int]
    alternative_probe_support_counts: Mapping[str, int]
    bounded_created_window_fraction: float
    bounded_created_duration_fraction: float
    source_content_sha256: str
    census_id: str = ""
    schema_version: str = TRIANGLE_SUPPORT_CENSUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, TRIANGLE_SUPPORT_CENSUS_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "candidate_id", _required_text(self.candidate_id)
        )
        start = _int64(self.start_ns, "start_ns")
        end = _int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("triangle support census range is empty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        expected_keys = {item.value for item in TriangleSupportClass}
        counts = _nonnegative_named_mapping(self.window_counts, expected_keys)
        durations = _nonnegative_named_mapping(
            self.duration_ns_by_support_class, expected_keys
        )
        if sum(durations.values()) != end - start:
            raise ValueError("triangle support census duration is incomplete")
        object.__setattr__(self, "window_counts", counts)
        object.__setattr__(self, "duration_ns_by_support_class", durations)
        for name in (
            "exact_event_sequence_support",
            "bounded_prior_support",
            "bounded_prior_only_support",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        selected = _triangle_int_mapping(
            self.selected_probe_leg_counts, "selected_probe_leg_counts"
        )
        alternatives = _triangle_int_mapping(
            self.alternative_probe_support_counts,
            "alternative_probe_support_counts",
        )
        if any(
            value < 0 for value in (*selected.values(), *alternatives.values())
        ):
            raise ValueError("triangle support probe count is negative")
        object.__setattr__(self, "selected_probe_leg_counts", selected)
        object.__setattr__(
            self, "alternative_probe_support_counts", alternatives
        )
        for name in (
            "bounded_created_window_fraction",
            "bounded_created_duration_fraction",
        ):
            object.__setattr__(
                self, name, _unit_float(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "source_content_sha256",
            _sha256(self.source_content_sha256, "source_content_sha256"),
        )
        expected = _stable_id("triangle-support-census", self.payload())
        if self.census_id and self.census_id != expected:
            raise ValueError("triangle support census identity differs")
        object.__setattr__(self, "census_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "window_counts": dict(self.window_counts),
            "duration_ns_by_support_class": dict(
                self.duration_ns_by_support_class
            ),
            "exact_event_sequence_support": self.exact_event_sequence_support,
            "bounded_prior_support": self.bounded_prior_support,
            "bounded_prior_only_support": self.bounded_prior_only_support,
            "selected_probe_leg_counts": dict(self.selected_probe_leg_counts),
            "alternative_probe_support_counts": dict(
                self.alternative_probe_support_counts
            ),
            "bounded_created_window_fraction": (
                self.bounded_created_window_fraction
            ),
            "bounded_created_duration_fraction": (
                self.bounded_created_duration_fraction
            ),
            "source_content_sha256": self.source_content_sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "census_id": self.census_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TriangleSupportCensusV1:
        return cls(
            candidate_id=str(data.get("candidate_id", "")),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            window_counts=_int_mapping(data.get("window_counts")),
            duration_ns_by_support_class=_int_mapping(
                data.get("duration_ns_by_support_class")
            ),
            exact_event_sequence_support=_strict_int(
                data.get("exact_event_sequence_support"),
                "exact_event_sequence_support",
            ),
            bounded_prior_support=_strict_int(
                data.get("bounded_prior_support"), "bounded_prior_support"
            ),
            bounded_prior_only_support=_strict_int(
                data.get("bounded_prior_only_support"),
                "bounded_prior_only_support",
            ),
            selected_probe_leg_counts=_int_mapping(
                data.get("selected_probe_leg_counts")
            ),
            alternative_probe_support_counts=_int_mapping(
                data.get("alternative_probe_support_counts")
            ),
            bounded_created_window_fraction=_finite_float(
                data.get("bounded_created_window_fraction"),
                "bounded_created_window_fraction",
            ),
            bounded_created_duration_fraction=_finite_float(
                data.get("bounded_created_duration_fraction"),
                "bounded_created_duration_fraction",
            ),
            source_content_sha256=str(data.get("source_content_sha256", "")),
            census_id=str(data.get("census_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleQuoteAgeSliceV1:
    """Nearest-rank quote-age distribution for one required evidence slice."""

    dimension: str
    key: str
    support_class: TriangleSupportClass
    sample_count: int
    age_quantiles_ns: Mapping[str, int]
    maximum_age_ns: int
    selected_tuple_content_sha256: str
    slice_id: str = ""
    schema_version: str = TRIANGLE_QUOTE_AGE_SLICE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, TRIANGLE_QUOTE_AGE_SLICE_SCHEMA_VERSION
        )
        dimension = _required_text(self.dimension)
        if dimension not in REQUIRED_AGE_SLICE_DIMENSIONS:
            raise ValueError("unsupported quote-age slice dimension")
        object.__setattr__(self, "dimension", dimension)
        object.__setattr__(self, "key", _required_text(self.key))
        support = TriangleSupportClass(self.support_class)
        if support not in {
            TriangleSupportClass.EXACT,
            TriangleSupportClass.BOUNDED_PRIOR_ONLY,
        }:
            raise ValueError("quote-age slice needs executable support")
        object.__setattr__(self, "support_class", support)
        count = _positive_int(self.sample_count, "sample_count")
        object.__setattr__(self, "sample_count", count)
        quantiles = _quantile_mapping(self.age_quantiles_ns)
        maximum = _nonnegative_int(self.maximum_age_ns, "maximum_age_ns")
        if quantiles["p100"] != maximum:
            raise ValueError("quote-age maximum differs from p100")
        object.__setattr__(self, "age_quantiles_ns", quantiles)
        object.__setattr__(self, "maximum_age_ns", maximum)
        object.__setattr__(
            self,
            "selected_tuple_content_sha256",
            _sha256(
                self.selected_tuple_content_sha256,
                "selected_tuple_content_sha256",
            ),
        )
        expected = _stable_id("triangle-quote-age-slice", self.payload())
        if self.slice_id and self.slice_id != expected:
            raise ValueError("quote-age slice identity differs")
        object.__setattr__(self, "slice_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dimension": self.dimension,
            "key": self.key,
            "support_class": self.support_class.value,
            "sample_count": self.sample_count,
            "age_quantiles_ns": dict(self.age_quantiles_ns),
            "maximum_age_ns": self.maximum_age_ns,
            "selected_tuple_content_sha256": (
                self.selected_tuple_content_sha256
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "slice_id": self.slice_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TriangleQuoteAgeSliceV1:
        return cls(
            dimension=str(data.get("dimension", "")),
            key=str(data.get("key", "")),
            support_class=TriangleSupportClass(
                str(data.get("support_class", ""))
            ),
            sample_count=_strict_int(data.get("sample_count"), "sample_count"),
            age_quantiles_ns=_int_mapping(data.get("age_quantiles_ns")),
            maximum_age_ns=_strict_int(
                data.get("maximum_age_ns"), "maximum_age_ns"
            ),
            selected_tuple_content_sha256=str(
                data.get("selected_tuple_content_sha256", "")
            ),
            slice_id=str(data.get("slice_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleAlignmentMetricToleranceV1:
    """Hard or advisory exact-versus-bounded metric tolerance."""

    metric_name: str
    absolute_tolerance: float
    relative_tolerance: float
    severity: TriangleToleranceSeverity
    tolerance_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_METRIC_TOLERANCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            TRIANGLE_ALIGNMENT_METRIC_TOLERANCE_SCHEMA_VERSION,
        )
        metric = _required_text(self.metric_name)
        if metric not in REQUIRED_ALIGNMENT_METRICS:
            raise ValueError("unsupported alignment sensitivity metric")
        object.__setattr__(self, "metric_name", metric)
        for name in ("absolute_tolerance", "relative_tolerance"):
            object.__setattr__(
                self, name, _nonnegative_finite(getattr(self, name), name)
            )
        object.__setattr__(
            self, "severity", TriangleToleranceSeverity(self.severity)
        )
        expected = _stable_id(
            "triangle-alignment-metric-tolerance", self.payload()
        )
        if self.tolerance_id and self.tolerance_id != expected:
            raise ValueError("alignment metric tolerance identity differs")
        object.__setattr__(self, "tolerance_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "metric_name": self.metric_name,
            "absolute_tolerance": self.absolute_tolerance,
            "relative_tolerance": self.relative_tolerance,
            "severity": self.severity.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "tolerance_id": self.tolerance_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> TriangleAlignmentMetricToleranceV1:
        return cls(
            metric_name=str(data.get("metric_name", "")),
            absolute_tolerance=_finite_float(
                data.get("absolute_tolerance"), "absolute_tolerance"
            ),
            relative_tolerance=_finite_float(
                data.get("relative_tolerance"), "relative_tolerance"
            ),
            severity=TriangleToleranceSeverity(str(data.get("severity", ""))),
            tolerance_id=str(data.get("tolerance_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleAlignmentOutcomeV1:
    """One otherwise-identical synthetic-output alignment treatment."""

    candidate_id: str
    source_window_id: str
    experiment_identity_id: str
    semantic_member_id: str
    observation_scenario_id: str
    policy: TriangleAlignmentPolicy
    configured_max_age_ns: int
    alignment_evidence_id: str
    source_content_sha256: str
    output_content_sha256: str
    metrics: Mapping[str, float]
    validation_only: bool
    observed_only_residual_immutable: bool
    synthetic_involved_residual_passed: bool
    outcome_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_OUTCOME_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, TRIANGLE_ALIGNMENT_OUTCOME_SCHEMA_VERSION
        )
        for name in (
            "candidate_id",
            "source_window_id",
            "experiment_identity_id",
            "semantic_member_id",
            "observation_scenario_id",
            "alignment_evidence_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        policy = TriangleAlignmentPolicy(self.policy)
        maximum = _nonnegative_int(
            self.configured_max_age_ns, "configured_max_age_ns"
        )
        if policy is TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE and maximum:
            raise ValueError("exact outcome cannot declare bounded age")
        if policy is TriangleAlignmentPolicy.BOUNDED_PRIOR and not maximum:
            raise ValueError("bounded outcome requires positive maximum age")
        object.__setattr__(self, "policy", policy)
        object.__setattr__(self, "configured_max_age_ns", maximum)
        for name in ("source_content_sha256", "output_content_sha256"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        metrics = _metric_mapping(self.metrics, REQUIRED_ALIGNMENT_METRICS)
        object.__setattr__(self, "metrics", metrics)
        for name in (
            "validation_only",
            "observed_only_residual_immutable",
            "synthetic_involved_residual_passed",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be boolean")
        expected = _stable_id("triangle-alignment-outcome", self.payload())
        if self.outcome_id and self.outcome_id != expected:
            raise ValueError("alignment outcome identity differs")
        object.__setattr__(self, "outcome_id", expected)

    def treatment_key(self) -> tuple[str, str, str, str]:
        return (
            self.source_window_id,
            self.experiment_identity_id,
            self.semantic_member_id,
            self.observation_scenario_id,
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "source_window_id": self.source_window_id,
            "experiment_identity_id": self.experiment_identity_id,
            "semantic_member_id": self.semantic_member_id,
            "observation_scenario_id": self.observation_scenario_id,
            "policy": self.policy.value,
            "configured_max_age_ns": self.configured_max_age_ns,
            "alignment_evidence_id": self.alignment_evidence_id,
            "source_content_sha256": self.source_content_sha256,
            "output_content_sha256": self.output_content_sha256,
            "metrics": dict(self.metrics),
            "validation_only": self.validation_only,
            "observed_only_residual_immutable": (
                self.observed_only_residual_immutable
            ),
            "synthetic_involved_residual_passed": (
                self.synthetic_involved_residual_passed
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "outcome_id": self.outcome_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TriangleAlignmentOutcomeV1:
        return cls(
            candidate_id=str(data.get("candidate_id", "")),
            source_window_id=str(data.get("source_window_id", "")),
            experiment_identity_id=str(data.get("experiment_identity_id", "")),
            semantic_member_id=str(data.get("semantic_member_id", "")),
            observation_scenario_id=str(
                data.get("observation_scenario_id", "")
            ),
            policy=TriangleAlignmentPolicy(str(data.get("policy", ""))),
            configured_max_age_ns=_strict_int(
                data.get("configured_max_age_ns"), "configured_max_age_ns"
            ),
            alignment_evidence_id=str(data.get("alignment_evidence_id", "")),
            source_content_sha256=str(data.get("source_content_sha256", "")),
            output_content_sha256=str(data.get("output_content_sha256", "")),
            metrics=_float_mapping(data.get("metrics")),
            validation_only=_strict_bool(
                data.get("validation_only"), "validation_only"
            ),
            observed_only_residual_immutable=_strict_bool(
                data.get("observed_only_residual_immutable"),
                "observed_only_residual_immutable",
            ),
            synthetic_involved_residual_passed=_strict_bool(
                data.get("synthetic_involved_residual_passed"),
                "synthetic_involved_residual_passed",
            ),
            outcome_id=str(data.get("outcome_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleAlignmentComparisonV1:
    """Derived paired comparison between two alignment outcomes."""

    baseline_outcome_id: str
    comparator_outcome_id: str
    source_window_id: str
    baseline_policy: TriangleAlignmentPolicy
    comparator_policy: TriangleAlignmentPolicy
    baseline_max_age_ns: int
    comparator_max_age_ns: int
    absolute_differences: Mapping[str, float]
    relative_differences: Mapping[str, float]
    hard_failures: tuple[str, ...]
    advisories: tuple[str, ...]
    comparison_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, TRIANGLE_ALIGNMENT_COMPARISON_SCHEMA_VERSION
        )
        for name in (
            "baseline_outcome_id",
            "comparator_outcome_id",
            "source_window_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "baseline_policy",
            TriangleAlignmentPolicy(self.baseline_policy),
        )
        object.__setattr__(
            self,
            "comparator_policy",
            TriangleAlignmentPolicy(self.comparator_policy),
        )
        for name in ("baseline_max_age_ns", "comparator_max_age_ns"):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        absolute = _metric_mapping(
            self.absolute_differences, REQUIRED_ALIGNMENT_METRICS
        )
        relative = _metric_mapping(
            self.relative_differences, REQUIRED_ALIGNMENT_METRICS
        )
        if any(value < 0 for value in (*absolute.values(), *relative.values())):
            raise ValueError("alignment comparison difference is negative")
        object.__setattr__(self, "absolute_differences", absolute)
        object.__setattr__(self, "relative_differences", relative)
        object.__setattr__(
            self, "hard_failures", _text_tuple(self.hard_failures)
        )
        object.__setattr__(self, "advisories", _text_tuple(self.advisories))
        expected = _stable_id("triangle-alignment-comparison", self.payload())
        if self.comparison_id and self.comparison_id != expected:
            raise ValueError("alignment comparison identity differs")
        object.__setattr__(self, "comparison_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "baseline_outcome_id": self.baseline_outcome_id,
            "comparator_outcome_id": self.comparator_outcome_id,
            "source_window_id": self.source_window_id,
            "baseline_policy": self.baseline_policy.value,
            "comparator_policy": self.comparator_policy.value,
            "baseline_max_age_ns": self.baseline_max_age_ns,
            "comparator_max_age_ns": self.comparator_max_age_ns,
            "absolute_differences": dict(self.absolute_differences),
            "relative_differences": dict(self.relative_differences),
            "hard_failures": list(self.hard_failures),
            "advisories": list(self.advisories),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "comparison_id": self.comparison_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> TriangleAlignmentComparisonV1:
        return cls(
            baseline_outcome_id=str(data.get("baseline_outcome_id", "")),
            comparator_outcome_id=str(data.get("comparator_outcome_id", "")),
            source_window_id=str(data.get("source_window_id", "")),
            baseline_policy=TriangleAlignmentPolicy(
                str(data.get("baseline_policy", ""))
            ),
            comparator_policy=TriangleAlignmentPolicy(
                str(data.get("comparator_policy", ""))
            ),
            baseline_max_age_ns=_strict_int(
                data.get("baseline_max_age_ns"), "baseline_max_age_ns"
            ),
            comparator_max_age_ns=_strict_int(
                data.get("comparator_max_age_ns"), "comparator_max_age_ns"
            ),
            absolute_differences=_float_mapping(
                data.get("absolute_differences")
            ),
            relative_differences=_float_mapping(
                data.get("relative_differences")
            ),
            hard_failures=_string_tuple(data.get("hard_failures")),
            advisories=_string_tuple(data.get("advisories")),
            comparison_id=str(data.get("comparison_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleAlignmentAgeRuleV1:
    """Predeclared age-conditioned output-coherence limit or refusal."""

    lower_age_ns: int
    upper_age_ns: int
    action: TriangleAgeRuleAction
    maximum_synthetic_residual: float
    maximum_projection_burden: float
    maximum_relative_sensitivity: float
    rule_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_AGE_RULE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, TRIANGLE_ALIGNMENT_AGE_RULE_SCHEMA_VERSION
        )
        lower = _nonnegative_int(self.lower_age_ns, "lower_age_ns")
        upper = _positive_int(self.upper_age_ns, "upper_age_ns")
        if upper <= lower:
            raise ValueError("triangle alignment age rule is empty")
        object.__setattr__(self, "lower_age_ns", lower)
        object.__setattr__(self, "upper_age_ns", upper)
        object.__setattr__(self, "action", TriangleAgeRuleAction(self.action))
        for name in (
            "maximum_synthetic_residual",
            "maximum_projection_burden",
            "maximum_relative_sensitivity",
        ):
            object.__setattr__(
                self, name, _nonnegative_finite(getattr(self, name), name)
            )
        expected = _stable_id("triangle-alignment-age-rule", self.payload())
        if self.rule_id and self.rule_id != expected:
            raise ValueError("alignment age rule identity differs")
        object.__setattr__(self, "rule_id", expected)

    def owns(self, age_ns: int) -> bool:
        return self.lower_age_ns <= age_ns < self.upper_age_ns

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "lower_age_ns": self.lower_age_ns,
            "upper_age_ns": self.upper_age_ns,
            "action": self.action.value,
            "maximum_synthetic_residual": self.maximum_synthetic_residual,
            "maximum_projection_burden": self.maximum_projection_burden,
            "maximum_relative_sensitivity": self.maximum_relative_sensitivity,
            "upper_bound_exclusive": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "rule_id": self.rule_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TriangleAlignmentAgeRuleV1:
        if data.get("upper_bound_exclusive") is not True:
            raise ValueError("alignment age rule boundary policy differs")
        return cls(
            lower_age_ns=_strict_int(data.get("lower_age_ns"), "lower_age_ns"),
            upper_age_ns=_strict_int(data.get("upper_age_ns"), "upper_age_ns"),
            action=TriangleAgeRuleAction(str(data.get("action", ""))),
            maximum_synthetic_residual=_finite_float(
                data.get("maximum_synthetic_residual"),
                "maximum_synthetic_residual",
            ),
            maximum_projection_burden=_finite_float(
                data.get("maximum_projection_burden"),
                "maximum_projection_burden",
            ),
            maximum_relative_sensitivity=_finite_float(
                data.get("maximum_relative_sensitivity"),
                "maximum_relative_sensitivity",
            ),
            rule_id=str(data.get("rule_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleAlignmentResidualBinV1:
    """Observed and synthetic residual/burden relation for one age bin."""

    lower_age_ns: int
    upper_age_ns: int
    sample_count: int
    observed_only_residual_mean: float
    observed_only_residual_maximum: float
    synthetic_post_projection_residual_mean: float
    synthetic_post_projection_residual_maximum: float
    projection_burden_mean: float
    projection_burden_maximum: float
    observed_evidence_content_sha256: str
    observed_only_residual_immutable: bool
    synthetic_involved_residual_passed: bool
    bin_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_RESIDUAL_BIN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, TRIANGLE_ALIGNMENT_RESIDUAL_BIN_SCHEMA_VERSION
        )
        lower = _nonnegative_int(self.lower_age_ns, "lower_age_ns")
        upper = _positive_int(self.upper_age_ns, "upper_age_ns")
        if upper <= lower:
            raise ValueError("alignment residual age bin is empty")
        object.__setattr__(self, "lower_age_ns", lower)
        object.__setattr__(self, "upper_age_ns", upper)
        object.__setattr__(
            self,
            "sample_count",
            _positive_int(self.sample_count, "sample_count"),
        )
        for name in (
            "observed_only_residual_mean",
            "observed_only_residual_maximum",
            "synthetic_post_projection_residual_mean",
            "synthetic_post_projection_residual_maximum",
            "projection_burden_mean",
            "projection_burden_maximum",
        ):
            object.__setattr__(
                self, name, _nonnegative_finite(getattr(self, name), name)
            )
        if (
            self.observed_only_residual_mean
            > self.observed_only_residual_maximum
        ):
            raise ValueError("observed residual mean exceeds maximum")
        if (
            self.synthetic_post_projection_residual_mean
            > self.synthetic_post_projection_residual_maximum
        ):
            raise ValueError("synthetic residual mean exceeds maximum")
        if self.projection_burden_mean > self.projection_burden_maximum:
            raise ValueError("projection burden mean exceeds maximum")
        object.__setattr__(
            self,
            "observed_evidence_content_sha256",
            _sha256(
                self.observed_evidence_content_sha256,
                "observed_evidence_content_sha256",
            ),
        )
        for name in (
            "observed_only_residual_immutable",
            "synthetic_involved_residual_passed",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be boolean")
        expected = _stable_id("triangle-alignment-residual-bin", self.payload())
        if self.bin_id and self.bin_id != expected:
            raise ValueError("alignment residual bin identity differs")
        object.__setattr__(self, "bin_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "lower_age_ns": self.lower_age_ns,
            "upper_age_ns": self.upper_age_ns,
            "sample_count": self.sample_count,
            "observed_only_residual_mean": self.observed_only_residual_mean,
            "observed_only_residual_maximum": (
                self.observed_only_residual_maximum
            ),
            "synthetic_post_projection_residual_mean": (
                self.synthetic_post_projection_residual_mean
            ),
            "synthetic_post_projection_residual_maximum": (
                self.synthetic_post_projection_residual_maximum
            ),
            "projection_burden_mean": self.projection_burden_mean,
            "projection_burden_maximum": self.projection_burden_maximum,
            "observed_evidence_content_sha256": (
                self.observed_evidence_content_sha256
            ),
            "observed_only_residual_immutable": (
                self.observed_only_residual_immutable
            ),
            "synthetic_involved_residual_passed": (
                self.synthetic_involved_residual_passed
            ),
            "upper_bound_exclusive": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "bin_id": self.bin_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> TriangleAlignmentResidualBinV1:
        if data.get("upper_bound_exclusive") is not True:
            raise ValueError("alignment residual bin boundary policy differs")
        return cls(
            lower_age_ns=_strict_int(data.get("lower_age_ns"), "lower_age_ns"),
            upper_age_ns=_strict_int(data.get("upper_age_ns"), "upper_age_ns"),
            sample_count=_strict_int(data.get("sample_count"), "sample_count"),
            observed_only_residual_mean=_finite_float(
                data.get("observed_only_residual_mean"),
                "observed_only_residual_mean",
            ),
            observed_only_residual_maximum=_finite_float(
                data.get("observed_only_residual_maximum"),
                "observed_only_residual_maximum",
            ),
            synthetic_post_projection_residual_mean=_finite_float(
                data.get("synthetic_post_projection_residual_mean"),
                "synthetic_post_projection_residual_mean",
            ),
            synthetic_post_projection_residual_maximum=_finite_float(
                data.get("synthetic_post_projection_residual_maximum"),
                "synthetic_post_projection_residual_maximum",
            ),
            projection_burden_mean=_finite_float(
                data.get("projection_burden_mean"), "projection_burden_mean"
            ),
            projection_burden_maximum=_finite_float(
                data.get("projection_burden_maximum"),
                "projection_burden_maximum",
            ),
            observed_evidence_content_sha256=str(
                data.get("observed_evidence_content_sha256", "")
            ),
            observed_only_residual_immutable=_strict_bool(
                data.get("observed_only_residual_immutable"),
                "observed_only_residual_immutable",
            ),
            synthetic_involved_residual_passed=_strict_bool(
                data.get("synthetic_involved_residual_passed"),
                "synthetic_involved_residual_passed",
            ),
            bin_id=str(data.get("bin_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleAlignmentConsumptionReceiptV1:
    """Planner/runtime/validation/publication identity equality proof."""

    source_window_id: str
    planner_alignment_evidence_id: str
    planner_policy: TriangleAlignmentPolicy
    planner_max_age_ns: int
    planner_probe_leg: str
    planner_recommended_event_time_ns: int
    planner_tuple_content_sha256: str
    runtime_alignment_evidence_id: str
    runtime_policy: TriangleAlignmentPolicy
    runtime_max_age_ns: int
    runtime_probe_leg: str
    runtime_recommended_event_time_ns: int
    runtime_tuple_content_sha256: str
    validation_policy: TriangleAlignmentPolicy
    validation_max_age_ns: int
    publication_policy_id: str
    expected_policy_id: str
    publication_alignment_evidence_id: str
    atomic_publication: bool
    receipt_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_CONSUMPTION_RECEIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            TRIANGLE_ALIGNMENT_CONSUMPTION_RECEIPT_SCHEMA_VERSION,
        )
        for name in (
            "source_window_id",
            "planner_alignment_evidence_id",
            "runtime_alignment_evidence_id",
            "publication_policy_id",
            "expected_policy_id",
            "publication_alignment_evidence_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in ("planner_policy", "runtime_policy", "validation_policy"):
            object.__setattr__(
                self, name, TriangleAlignmentPolicy(getattr(self, name))
            )
        for name in (
            "planner_max_age_ns",
            "runtime_max_age_ns",
            "validation_max_age_ns",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        for name in ("planner_probe_leg", "runtime_probe_leg"):
            probe = _symbol(getattr(self, name))
            if probe not in TRIANGLE_SYMBOLS:
                raise ValueError(
                    "alignment consumption probe is outside triangle"
                )
            object.__setattr__(self, name, probe)
        for name in (
            "planner_recommended_event_time_ns",
            "runtime_recommended_event_time_ns",
        ):
            object.__setattr__(self, name, _int64(getattr(self, name), name))
        for name in (
            "planner_tuple_content_sha256",
            "runtime_tuple_content_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        if type(self.atomic_publication) is not bool:
            raise TypeError("atomic_publication must be boolean")
        expected = _stable_id(
            "triangle-alignment-consumption-receipt", self.payload()
        )
        if self.receipt_id and self.receipt_id != expected:
            raise ValueError("alignment consumption receipt identity differs")
        object.__setattr__(self, "receipt_id", expected)

    @property
    def matched(self) -> bool:
        return (
            self.planner_alignment_evidence_id
            == self.runtime_alignment_evidence_id
            == self.publication_alignment_evidence_id
            and self.planner_policy
            is self.runtime_policy
            is self.validation_policy
            and self.planner_max_age_ns
            == self.runtime_max_age_ns
            == self.validation_max_age_ns
            and self.planner_probe_leg == self.runtime_probe_leg
            and self.planner_recommended_event_time_ns
            == self.runtime_recommended_event_time_ns
            and self.planner_tuple_content_sha256
            == self.runtime_tuple_content_sha256
            and self.publication_policy_id == self.expected_policy_id
            and self.atomic_publication
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_window_id": self.source_window_id,
            "planner_alignment_evidence_id": self.planner_alignment_evidence_id,
            "planner_policy": self.planner_policy.value,
            "planner_max_age_ns": self.planner_max_age_ns,
            "planner_probe_leg": self.planner_probe_leg,
            "planner_recommended_event_time_ns": (
                self.planner_recommended_event_time_ns
            ),
            "planner_tuple_content_sha256": (self.planner_tuple_content_sha256),
            "runtime_alignment_evidence_id": self.runtime_alignment_evidence_id,
            "runtime_policy": self.runtime_policy.value,
            "runtime_max_age_ns": self.runtime_max_age_ns,
            "runtime_probe_leg": self.runtime_probe_leg,
            "runtime_recommended_event_time_ns": (
                self.runtime_recommended_event_time_ns
            ),
            "runtime_tuple_content_sha256": (self.runtime_tuple_content_sha256),
            "validation_policy": self.validation_policy.value,
            "validation_max_age_ns": self.validation_max_age_ns,
            "publication_policy_id": self.publication_policy_id,
            "expected_policy_id": self.expected_policy_id,
            "publication_alignment_evidence_id": (
                self.publication_alignment_evidence_id
            ),
            "atomic_publication": self.atomic_publication,
            "matched": self.matched,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "receipt_id": self.receipt_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> TriangleAlignmentConsumptionReceiptV1:
        result = cls(
            source_window_id=str(data.get("source_window_id", "")),
            planner_alignment_evidence_id=str(
                data.get("planner_alignment_evidence_id", "")
            ),
            planner_policy=TriangleAlignmentPolicy(
                str(data.get("planner_policy", ""))
            ),
            planner_max_age_ns=_strict_int(
                data.get("planner_max_age_ns"), "planner_max_age_ns"
            ),
            planner_probe_leg=str(data.get("planner_probe_leg", "")),
            planner_recommended_event_time_ns=_strict_int(
                data.get("planner_recommended_event_time_ns"),
                "planner_recommended_event_time_ns",
            ),
            planner_tuple_content_sha256=str(
                data.get("planner_tuple_content_sha256", "")
            ),
            runtime_alignment_evidence_id=str(
                data.get("runtime_alignment_evidence_id", "")
            ),
            runtime_policy=TriangleAlignmentPolicy(
                str(data.get("runtime_policy", ""))
            ),
            runtime_max_age_ns=_strict_int(
                data.get("runtime_max_age_ns"), "runtime_max_age_ns"
            ),
            runtime_probe_leg=str(data.get("runtime_probe_leg", "")),
            runtime_recommended_event_time_ns=_strict_int(
                data.get("runtime_recommended_event_time_ns"),
                "runtime_recommended_event_time_ns",
            ),
            runtime_tuple_content_sha256=str(
                data.get("runtime_tuple_content_sha256", "")
            ),
            validation_policy=TriangleAlignmentPolicy(
                str(data.get("validation_policy", ""))
            ),
            validation_max_age_ns=_strict_int(
                data.get("validation_max_age_ns"), "validation_max_age_ns"
            ),
            publication_policy_id=str(data.get("publication_policy_id", "")),
            expected_policy_id=str(data.get("expected_policy_id", "")),
            publication_alignment_evidence_id=str(
                data.get("publication_alignment_evidence_id", "")
            ),
            atomic_publication=_strict_bool(
                data.get("atomic_publication"), "atomic_publication"
            ),
            receipt_id=str(data.get("receipt_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("matched") is not result.matched:
            raise ValueError("alignment consumption match decision differs")
        return result


@dataclass(frozen=True, slots=True)
class TriangleAlignmentQualificationPolicyV1:
    """Predeclared support, sensitivity, and age-conditioned release policy."""

    maximum_age_ns: int
    sensitivity_age_ceilings_ns: tuple[int, ...]
    minimum_alignment_support: int
    minimum_sensitivity_pairs_per_window: int
    metric_tolerances: tuple[TriangleAlignmentMetricToleranceV1, ...]
    age_rules: tuple[TriangleAlignmentAgeRuleV1, ...]
    required_age_slice_dimensions: tuple[str, ...] = (
        REQUIRED_AGE_SLICE_DIMENSIONS
    )
    policy_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_QUALIFICATION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            TRIANGLE_ALIGNMENT_QUALIFICATION_POLICY_SCHEMA_VERSION,
        )
        maximum = _positive_int(self.maximum_age_ns, "maximum_age_ns")
        object.__setattr__(self, "maximum_age_ns", maximum)
        ceilings = tuple(
            sorted(
                {
                    _positive_int(item, "sensitivity_age_ceiling_ns")
                    for item in self.sensitivity_age_ceilings_ns
                }
            )
        )
        if len(ceilings) < 3 or ceilings[-1] != maximum:
            raise ValueError(
                "alignment sensitivity needs at least three ceilings ending at policy maximum"
            )
        object.__setattr__(self, "sensitivity_age_ceilings_ns", ceilings)
        for name in (
            "minimum_alignment_support",
            "minimum_sensitivity_pairs_per_window",
        ):
            object.__setattr__(
                self, name, _positive_int(getattr(self, name), name)
            )
        tolerances = tuple(
            sorted(self.metric_tolerances, key=lambda item: item.metric_name)
        )
        if {
            item.metric_name for item in tolerances
        } != REQUIRED_ALIGNMENT_METRICS:
            raise ValueError(
                "alignment policy does not cover every required metric"
            )
        object.__setattr__(self, "metric_tolerances", tolerances)
        rules = tuple(
            sorted(self.age_rules, key=lambda item: item.lower_age_ns)
        )
        if not rules or rules[0].lower_age_ns != 0:
            raise ValueError("alignment age rules must begin at zero")
        if any(
            left.upper_age_ns != right.lower_age_ns
            for left, right in pairwise(rules)
        ):
            raise ValueError("alignment age rules are not contiguous")
        if rules[-1].upper_age_ns != maximum + 1:
            raise ValueError(
                "alignment age rules must cover the inclusive maximum"
            )
        if not any(
            item.action is TriangleAgeRuleAction.ADMIT for item in rules
        ):
            raise ValueError("alignment policy has no admitted age region")
        object.__setattr__(self, "age_rules", rules)
        dimensions = tuple(
            sorted(
                {
                    _required_text(item)
                    for item in self.required_age_slice_dimensions
                }
            )
        )
        if set(dimensions) != set(REQUIRED_AGE_SLICE_DIMENSIONS):
            raise ValueError(
                "alignment policy omits a required quote-age slice"
            )
        object.__setattr__(self, "required_age_slice_dimensions", dimensions)
        expected = _stable_id(
            "triangle-alignment-qualification-policy", self.payload()
        )
        if self.policy_id and self.policy_id != expected:
            raise ValueError("alignment qualification policy identity differs")
        object.__setattr__(self, "policy_id", expected)

    def tolerance(self, metric_name: str) -> TriangleAlignmentMetricToleranceV1:
        return next(
            item
            for item in self.metric_tolerances
            if item.metric_name == metric_name
        )

    def age_rule(self, age_ns: int) -> TriangleAlignmentAgeRuleV1:
        normalized = _nonnegative_int(age_ns, "age_ns")
        try:
            return next(
                item for item in self.age_rules if item.owns(normalized)
            )
        except StopIteration as err:
            raise ValueError(
                "quote age lies outside qualification policy"
            ) from err

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "maximum_age_ns": self.maximum_age_ns,
            "sensitivity_age_ceilings_ns": list(
                self.sensitivity_age_ceilings_ns
            ),
            "minimum_alignment_support": self.minimum_alignment_support,
            "minimum_sensitivity_pairs_per_window": (
                self.minimum_sensitivity_pairs_per_window
            ),
            "metric_tolerances": [
                item.to_dict() for item in self.metric_tolerances
            ],
            "age_rules": [item.to_dict() for item in self.age_rules],
            "required_age_slice_dimensions": list(
                self.required_age_slice_dimensions
            ),
            "maximizing_support_is_not_sufficient": True,
            "future_event_allowed": False,
            "silent_age_widening_allowed": False,
            "one_leg_outage_may_be_infilled": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> TriangleAlignmentQualificationPolicyV1:
        for key in (
            "maximizing_support_is_not_sufficient",
            "future_event_allowed",
            "silent_age_widening_allowed",
            "one_leg_outage_may_be_infilled",
        ):
            expected = key == "maximizing_support_is_not_sufficient"
            if data.get(key) is not expected:
                raise ValueError("alignment qualification safeguard differs")
        return cls(
            maximum_age_ns=_strict_int(
                data.get("maximum_age_ns"), "maximum_age_ns"
            ),
            sensitivity_age_ceilings_ns=tuple(
                _strict_int(item, "sensitivity_age_ceiling_ns")
                for item in _sequence(data.get("sensitivity_age_ceilings_ns"))
            ),
            minimum_alignment_support=_strict_int(
                data.get("minimum_alignment_support"),
                "minimum_alignment_support",
            ),
            minimum_sensitivity_pairs_per_window=_strict_int(
                data.get("minimum_sensitivity_pairs_per_window"),
                "minimum_sensitivity_pairs_per_window",
            ),
            metric_tolerances=tuple(
                TriangleAlignmentMetricToleranceV1.from_dict(_mapping(item))
                for item in _sequence(data.get("metric_tolerances"))
            ),
            age_rules=tuple(
                TriangleAlignmentAgeRuleV1.from_dict(_mapping(item))
                for item in _sequence(data.get("age_rules"))
            ),
            required_age_slice_dimensions=_string_tuple(
                data.get("required_age_slice_dimensions")
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class TriangleAlignmentQualificationV1:
    """Complete candidate-bound exact/bounded alignment qualification."""

    candidate_id: str
    release_candidate_ref: ArtifactRef
    policy: TriangleAlignmentQualificationPolicyV1
    window_evidence: tuple[TriangleAlignmentWindowEvidenceV1, ...]
    census: TriangleSupportCensusV1
    quote_age_slices: tuple[TriangleQuoteAgeSliceV1, ...]
    outcomes: tuple[TriangleAlignmentOutcomeV1, ...]
    comparisons: tuple[TriangleAlignmentComparisonV1, ...]
    residual_bins: tuple[TriangleAlignmentResidualBinV1, ...]
    consumption_receipts: tuple[TriangleAlignmentConsumptionReceiptV1, ...]
    status: TriangleQualificationStatus
    failure_reasons: tuple[str, ...]
    advisories: tuple[str, ...]
    created_at: str
    qualification_id: str = ""
    schema_version: str = TRIANGLE_ALIGNMENT_QUALIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, TRIANGLE_ALIGNMENT_QUALIFICATION_SCHEMA_VERSION
        )
        candidate_id = _required_text(self.candidate_id)
        object.__setattr__(self, "candidate_id", candidate_id)
        _verify_release_candidate_ref(self.release_candidate_ref, candidate_id)
        if not isinstance(self.policy, TriangleAlignmentQualificationPolicyV1):
            raise TypeError("alignment qualification policy has invalid type")
        windows = tuple(
            sorted(self.window_evidence, key=lambda item: item.start_ns)
        )
        if not windows or len(windows) > MAX_ALIGNMENT_WINDOWS:
            raise ValueError("alignment qualification window count is invalid")
        _validate_evidence_cover(windows, candidate_id)
        object.__setattr__(self, "window_evidence", windows)
        expected_census = build_triangle_support_census(windows)
        if self.census.census_id != expected_census.census_id:
            raise ValueError("alignment qualification census differs")
        expected_slices = build_triangle_quote_age_slices(windows)
        slices = tuple(
            sorted(self.quote_age_slices, key=lambda item: item.slice_id)
        )
        if tuple(item.slice_id for item in slices) != tuple(
            item.slice_id for item in expected_slices
        ):
            raise ValueError("alignment qualification quote-age slices differ")
        object.__setattr__(self, "quote_age_slices", slices)
        outcomes = tuple(
            sorted(self.outcomes, key=lambda item: item.outcome_id)
        )
        if len(outcomes) > MAX_ALIGNMENT_OUTCOMES:
            raise ValueError("alignment qualification outcome bound exceeded")
        if any(item.candidate_id != candidate_id for item in outcomes):
            raise ValueError("alignment outcome candidate differs")
        object.__setattr__(self, "outcomes", outcomes)
        expected_comparisons = compare_triangle_alignment_outcomes(
            outcomes, self.policy
        )
        comparisons = tuple(
            sorted(self.comparisons, key=lambda item: item.comparison_id)
        )
        if tuple(item.comparison_id for item in comparisons) != tuple(
            item.comparison_id for item in expected_comparisons
        ):
            raise ValueError("alignment qualification comparisons differ")
        object.__setattr__(self, "comparisons", comparisons)
        bins = tuple(
            sorted(self.residual_bins, key=lambda item: item.lower_age_ns)
        )
        if not bins or len(bins) > MAX_ALIGNMENT_RESIDUAL_BINS:
            raise ValueError(
                "alignment qualification residual bins are invalid"
            )
        if any(
            left.upper_age_ns > right.lower_age_ns
            for left, right in pairwise(bins)
        ):
            raise ValueError("alignment qualification residual bins overlap")
        object.__setattr__(self, "residual_bins", bins)
        receipts = tuple(
            sorted(
                self.consumption_receipts,
                key=lambda item: item.source_window_id,
            )
        )
        if len({item.source_window_id for item in receipts}) != len(receipts):
            raise ValueError(
                "alignment qualification repeats consumption receipt"
            )
        object.__setattr__(self, "consumption_receipts", receipts)
        calculated_failures, calculated_advisories = _qualification_findings(
            windows,
            self.policy,
            outcomes,
            comparisons,
            bins,
            receipts,
        )
        failures = _text_tuple(self.failure_reasons)
        advisories = _text_tuple(self.advisories)
        if (
            failures != calculated_failures
            or advisories != calculated_advisories
        ):
            raise ValueError("alignment qualification findings differ")
        expected_status = (
            TriangleQualificationStatus.FAIL
            if failures
            else TriangleQualificationStatus.PASS
        )
        status = TriangleQualificationStatus(self.status)
        if status is not expected_status:
            raise ValueError("alignment qualification status differs")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "failure_reasons", failures)
        object.__setattr__(self, "advisories", advisories)
        object.__setattr__(self, "created_at", _timestamp(self.created_at))
        expected = _stable_id(
            "triangle-alignment-qualification", self.payload()
        )
        if self.qualification_id and self.qualification_id != expected:
            raise ValueError("alignment qualification identity differs")
        object.__setattr__(self, "qualification_id", expected)

    @property
    def passed(self) -> bool:
        return self.status is TriangleQualificationStatus.PASS

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "release_candidate_ref": self.release_candidate_ref.to_dict(),
            "policy": self.policy.to_dict(),
            "window_evidence": [
                item.to_dict() for item in self.window_evidence
            ],
            "census": self.census.to_dict(),
            "quote_age_slices": [
                item.to_dict() for item in self.quote_age_slices
            ],
            "outcomes": [item.to_dict() for item in self.outcomes],
            "comparisons": [item.to_dict() for item in self.comparisons],
            "residual_bins": [item.to_dict() for item in self.residual_bins],
            "consumption_receipts": [
                item.to_dict() for item in self.consumption_receipts
            ],
            "status": self.status.value,
            "failure_reasons": list(self.failure_reasons),
            "advisories": list(self.advisories),
            "created_at": self.created_at,
            "observed_only_residual_role": "immutable_source_quality_evidence",
            "synthetic_involved_residual_role": "blocking_after_projection",
            "publication_identity_includes_policy_and_max_age": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "qualification_id": self.qualification_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> TriangleAlignmentQualificationV1:
        if data.get("observed_only_residual_role") != (
            "immutable_source_quality_evidence"
        ):
            raise ValueError("observed residual role differs")
        if data.get("synthetic_involved_residual_role") != (
            "blocking_after_projection"
        ):
            raise ValueError("synthetic residual role differs")
        if (
            data.get("publication_identity_includes_policy_and_max_age")
            is not True
        ):
            raise ValueError("alignment publication identity policy differs")
        return cls(
            candidate_id=str(data.get("candidate_id", "")),
            release_candidate_ref=ArtifactRef.from_dict(
                _mapping(data.get("release_candidate_ref"))
            ),
            policy=TriangleAlignmentQualificationPolicyV1.from_dict(
                _mapping(data.get("policy"))
            ),
            window_evidence=tuple(
                TriangleAlignmentWindowEvidenceV1.from_dict(_mapping(item))
                for item in _sequence(data.get("window_evidence"))
            ),
            census=TriangleSupportCensusV1.from_dict(
                _mapping(data.get("census"))
            ),
            quote_age_slices=tuple(
                TriangleQuoteAgeSliceV1.from_dict(_mapping(item))
                for item in _sequence(data.get("quote_age_slices"))
            ),
            outcomes=tuple(
                TriangleAlignmentOutcomeV1.from_dict(_mapping(item))
                for item in _sequence(data.get("outcomes"))
            ),
            comparisons=tuple(
                TriangleAlignmentComparisonV1.from_dict(_mapping(item))
                for item in _sequence(data.get("comparisons"))
            ),
            residual_bins=tuple(
                TriangleAlignmentResidualBinV1.from_dict(_mapping(item))
                for item in _sequence(data.get("residual_bins"))
            ),
            consumption_receipts=tuple(
                TriangleAlignmentConsumptionReceiptV1.from_dict(_mapping(item))
                for item in _sequence(data.get("consumption_receipts"))
            ),
            status=TriangleQualificationStatus(str(data.get("status", ""))),
            failure_reasons=_string_tuple(data.get("failure_reasons")),
            advisories=_string_tuple(data.get("advisories")),
            created_at=str(data.get("created_at", "")),
            qualification_id=str(data.get("qualification_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def analyze_triangle_alignment_window(
    window: TriangleAlignmentSourceWindowV1,
    policy: TriangleAlignmentQualificationPolicyV1,
) -> TriangleAlignmentWindowEvidenceV1:
    """Enumerate exact and every probe-leg bounded-prior treatment."""
    counts = Counter(item.symbol for item in window.events)
    source_counts = {symbol: counts[symbol] for symbol in TRIANGLE_SYMBOLS}
    empty_counts = {symbol: 0 for symbol in TRIANGLE_SYMBOLS}
    if window.source_state in {
        TriangleSourceWindowState.EMPTY,
        TriangleSourceWindowState.EXPECTED_CLOSURE,
    }:
        support_class = (
            TriangleSupportClass.EMPTY
            if window.source_state is TriangleSourceWindowState.EMPTY
            else TriangleSupportClass.EXPECTED_CLOSURE
        )
        return TriangleAlignmentWindowEvidenceV1(
            candidate_id=window.candidate_id,
            source_window_id=window.window_id,
            start_ns=window.start_ns,
            end_ns=window.end_ns,
            year=window.year,
            feed_epoch=window.feed_epoch,
            session=window.session,
            event_state=window.event_state,
            activity_stratum=window.activity_stratum,
            source_state=window.source_state,
            support_class=support_class,
            source_event_counts=source_counts,
            exact_event_sequence_support=0,
            bounded_support_by_probe_leg=empty_counts,
            bounded_only_support_by_probe_leg=empty_counts,
            selected_policy=None,
            selected_probe_leg=None,
            configured_max_age_ns=0,
            selected_tuples=(),
            source_content_sha256=window.source_content_sha256,
            recommended_event_time_ns=None,
        )
    if window.source_state is TriangleSourceWindowState.INCOMPLETE:
        return TriangleAlignmentWindowEvidenceV1(
            candidate_id=window.candidate_id,
            source_window_id=window.window_id,
            start_ns=window.start_ns,
            end_ns=window.end_ns,
            year=window.year,
            feed_epoch=window.feed_epoch,
            session=window.session,
            event_state=window.event_state,
            activity_stratum=window.activity_stratum,
            source_state=window.source_state,
            support_class=TriangleSupportClass.INCOMPLETE_SOURCE,
            source_event_counts=source_counts,
            exact_event_sequence_support=0,
            bounded_support_by_probe_leg=empty_counts,
            bounded_only_support_by_probe_leg=empty_counts,
            selected_policy=None,
            selected_probe_leg=None,
            configured_max_age_ns=0,
            selected_tuples=(),
            source_content_sha256=window.source_content_sha256,
            recommended_event_time_ns=None,
        )
    events_by_symbol = {
        symbol: tuple(item for item in window.events if item.symbol == symbol)
        for symbol in TRIANGLE_SYMBOLS
    }
    exact = _exact_tuples(window.window_id, events_by_symbol)
    bounded_by_probe = {
        symbol: _bounded_tuples(
            window.window_id,
            events_by_symbol,
            probe_symbol=symbol,
            maximum_age_ns=policy.maximum_age_ns,
        )
        for symbol in TRIANGLE_SYMBOLS
    }
    bounded_counts = {
        symbol: len(bounded_by_probe[symbol]) for symbol in TRIANGLE_SYMBOLS
    }
    bounded_only_counts = {
        symbol: sum(item.bounded_only for item in bounded_by_probe[symbol])
        for symbol in TRIANGLE_SYMBOLS
    }
    selected_policy: TriangleAlignmentPolicy | None = None
    selected_probe: str | None = None
    selected: tuple[TriangleAlignmentTupleV1, ...] = ()
    support_class = TriangleSupportClass.UNSUPPORTED_COMPLETE
    if len(exact) >= policy.minimum_alignment_support:
        support_class = TriangleSupportClass.EXACT
        selected_policy = TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE
        selected_probe = TRIANGLE_SYMBOLS[0]
        selected = exact
    else:
        selected_probe = min(
            TRIANGLE_SYMBOLS,
            key=lambda symbol: (
                -len(bounded_by_probe[symbol]),
                len(events_by_symbol[symbol]),
                symbol,
            ),
        )
        selected = bounded_by_probe[selected_probe]
        if len(selected) >= policy.minimum_alignment_support:
            support_class = TriangleSupportClass.BOUNDED_PRIOR_ONLY
            selected_policy = TriangleAlignmentPolicy.BOUNDED_PRIOR
        else:
            selected_probe = None
            selected = ()
    return TriangleAlignmentWindowEvidenceV1(
        candidate_id=window.candidate_id,
        source_window_id=window.window_id,
        start_ns=window.start_ns,
        end_ns=window.end_ns,
        year=window.year,
        feed_epoch=window.feed_epoch,
        session=window.session,
        event_state=window.event_state,
        activity_stratum=window.activity_stratum,
        source_state=window.source_state,
        support_class=support_class,
        source_event_counts=source_counts,
        exact_event_sequence_support=len(exact),
        bounded_support_by_probe_leg=bounded_counts,
        bounded_only_support_by_probe_leg=bounded_only_counts,
        selected_policy=selected_policy,
        selected_probe_leg=selected_probe,
        configured_max_age_ns=(
            policy.maximum_age_ns
            if selected_policy is TriangleAlignmentPolicy.BOUNDED_PRIOR
            else 0
        ),
        selected_tuples=selected,
        source_content_sha256=window.source_content_sha256,
        recommended_event_time_ns=(
            selected[len(selected) // 2].probe_time_ns if selected else None
        ),
    )


def build_triangle_support_census(
    evidence: Sequence[TriangleAlignmentWindowEvidenceV1],
) -> TriangleSupportCensusV1:
    """Aggregate a contiguous complete-range support decomposition."""
    windows = tuple(sorted(evidence, key=lambda item: item.start_ns))
    if not windows:
        raise ValueError("triangle support census requires evidence")
    _validate_evidence_cover(windows, windows[0].candidate_id)
    counts = Counter(item.support_class.value for item in windows)
    durations = Counter(
        {
            support.value: sum(
                item.end_ns - item.start_ns
                for item in windows
                if item.support_class is support
            )
            for support in TriangleSupportClass
        }
    )
    selected_probe = Counter(
        item.selected_probe_leg
        for item in windows
        if item.selected_probe_leg is not None
    )
    alternatives: Counter[str] = Counter()
    for item in windows:
        alternatives.update(item.bounded_support_by_probe_leg)
    complete = tuple(
        item
        for item in windows
        if item.source_state is TriangleSourceWindowState.AVAILABLE
    )
    bounded_windows = counts[TriangleSupportClass.BOUNDED_PRIOR_ONLY.value]
    bounded_duration = durations[TriangleSupportClass.BOUNDED_PRIOR_ONLY.value]
    complete_duration = sum(item.end_ns - item.start_ns for item in complete)
    source_digest = _content_sha256(
        [
            {
                "source_window_id": item.source_window_id,
                "source_content_sha256": item.source_content_sha256,
            }
            for item in windows
        ]
    )
    return TriangleSupportCensusV1(
        candidate_id=windows[0].candidate_id,
        start_ns=windows[0].start_ns,
        end_ns=windows[-1].end_ns,
        window_counts={
            item.value: counts[item.value] for item in TriangleSupportClass
        },
        duration_ns_by_support_class={
            item.value: durations[item.value] for item in TriangleSupportClass
        },
        exact_event_sequence_support=sum(
            item.exact_event_sequence_support for item in windows
        ),
        bounded_prior_support=sum(
            sum(item.bounded_support_by_probe_leg.values()) for item in windows
        ),
        bounded_prior_only_support=sum(
            sum(item.bounded_only_support_by_probe_leg.values())
            for item in windows
        ),
        selected_probe_leg_counts={
            symbol: selected_probe[symbol] for symbol in TRIANGLE_SYMBOLS
        },
        alternative_probe_support_counts={
            symbol: alternatives[symbol] for symbol in TRIANGLE_SYMBOLS
        },
        bounded_created_window_fraction=(
            bounded_windows / len(complete) if complete else 0.0
        ),
        bounded_created_duration_fraction=(
            bounded_duration / complete_duration if complete_duration else 0.0
        ),
        source_content_sha256=source_digest,
    )


def build_triangle_quote_age_slices(
    evidence: Sequence[TriangleAlignmentWindowEvidenceV1],
) -> tuple[TriangleQuoteAgeSliceV1, ...]:
    """Build all mandatory age slices from selected content-bound tuples."""
    grouped: dict[
        tuple[str, str, TriangleSupportClass],
        list[tuple[int, str]],
    ] = defaultdict(list)
    for item in evidence:
        if item.support_class not in {
            TriangleSupportClass.EXACT,
            TriangleSupportClass.BOUNDED_PRIOR_ONLY,
        }:
            continue
        for aligned in item.selected_tuples:
            dimensions = {
                "year": str(item.year),
                "feed_epoch": item.feed_epoch,
                "session": item.session,
                "event_state": item.event_state,
                "activity_stratum": item.activity_stratum,
            }
            for symbol in TRIANGLE_SYMBOLS:
                age = aligned.ages_ns[symbol]
                grouped[
                    (
                        "symbol_probe_leg",
                        f"symbol={symbol};probe_leg={aligned.probe_symbol}",
                        item.support_class,
                    )
                ].append((age, aligned.tuple_id))
                for dimension, key in dimensions.items():
                    grouped[(dimension, key, item.support_class)].append(
                        (age, aligned.tuple_id)
                    )
    slices = []
    for (dimension, key, support), samples in grouped.items():
        ages = sorted(item[0] for item in samples)
        tuple_digest = _content_sha256(sorted(item[1] for item in samples))
        slices.append(
            TriangleQuoteAgeSliceV1(
                dimension=dimension,
                key=key,
                support_class=support,
                sample_count=len(ages),
                age_quantiles_ns=_age_quantiles(ages),
                maximum_age_ns=ages[-1],
                selected_tuple_content_sha256=tuple_digest,
            )
        )
    return tuple(sorted(slices, key=lambda item: item.slice_id))


def compare_triangle_alignment_outcomes(
    outcomes: Sequence[TriangleAlignmentOutcomeV1],
    policy: TriangleAlignmentQualificationPolicyV1,
) -> tuple[TriangleAlignmentComparisonV1, ...]:
    """Pair otherwise-identical outcomes in increasing age-treatment order."""
    grouped: dict[
        tuple[str, str, str, str], list[TriangleAlignmentOutcomeV1]
    ] = defaultdict(list)
    for item in outcomes:
        grouped[item.treatment_key()].append(item)
    comparisons: list[TriangleAlignmentComparisonV1] = []
    for group in grouped.values():
        ordered = sorted(
            group,
            key=lambda item: (
                (
                    0
                    if item.policy
                    is TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE
                    else 1
                ),
                item.configured_max_age_ns,
                item.outcome_id,
            ),
        )
        seen_treatments: set[tuple[TriangleAlignmentPolicy, int]] = set()
        for item in ordered:
            treatment = (item.policy, item.configured_max_age_ns)
            if treatment in seen_treatments:
                raise ValueError("duplicate outcome treatment for one identity")
            seen_treatments.add(treatment)
        for baseline, comparator in pairwise(ordered):
            absolute = {
                metric: abs(
                    comparator.metrics[metric] - baseline.metrics[metric]
                )
                for metric in REQUIRED_ALIGNMENT_METRICS
            }
            relative = {
                metric: absolute[metric]
                / max(abs(baseline.metrics[metric]), 1e-12)
                for metric in REQUIRED_ALIGNMENT_METRICS
            }
            hard: list[str] = []
            advisory: list[str] = []
            for metric in sorted(REQUIRED_ALIGNMENT_METRICS):
                tolerance = policy.tolerance(metric)
                if (
                    absolute[metric] > tolerance.absolute_tolerance
                    and relative[metric] > tolerance.relative_tolerance
                ):
                    target = (
                        hard
                        if tolerance.severity is TriangleToleranceSeverity.HARD
                        else advisory
                    )
                    target.append(f"{metric}:alignment_tolerance_exceeded")
            comparisons.append(
                TriangleAlignmentComparisonV1(
                    baseline_outcome_id=baseline.outcome_id,
                    comparator_outcome_id=comparator.outcome_id,
                    source_window_id=baseline.source_window_id,
                    baseline_policy=baseline.policy,
                    comparator_policy=comparator.policy,
                    baseline_max_age_ns=baseline.configured_max_age_ns,
                    comparator_max_age_ns=comparator.configured_max_age_ns,
                    absolute_differences=absolute,
                    relative_differences=relative,
                    hard_failures=tuple(hard),
                    advisories=tuple(advisory),
                )
            )
    return tuple(sorted(comparisons, key=lambda item: item.comparison_id))


def qualify_triangle_alignment(
    *,
    candidate_id: str,
    release_candidate_ref: ArtifactRef,
    policy: TriangleAlignmentQualificationPolicyV1,
    source_windows: Sequence[TriangleAlignmentSourceWindowV1],
    outcomes: Sequence[TriangleAlignmentOutcomeV1],
    residual_bins: Sequence[TriangleAlignmentResidualBinV1],
    consumption_receipts: Sequence[TriangleAlignmentConsumptionReceiptV1],
    created_at: str,
) -> TriangleAlignmentQualificationV1:
    """Recompute the complete support evidence and apply release gates."""
    _verify_release_candidate_ref(release_candidate_ref, candidate_id)
    windows = tuple(sorted(source_windows, key=lambda item: item.start_ns))
    _validate_source_window_cover(windows, candidate_id)
    evidence = tuple(
        analyze_triangle_alignment_window(item, policy) for item in windows
    )
    census = build_triangle_support_census(evidence)
    slices = build_triangle_quote_age_slices(evidence)
    normalized_outcomes = tuple(outcomes)
    comparisons = compare_triangle_alignment_outcomes(
        normalized_outcomes, policy
    )
    failures, advisories = _qualification_findings(
        evidence,
        policy,
        normalized_outcomes,
        comparisons,
        tuple(residual_bins),
        tuple(consumption_receipts),
    )
    return TriangleAlignmentQualificationV1(
        candidate_id=candidate_id,
        release_candidate_ref=release_candidate_ref,
        policy=policy,
        window_evidence=evidence,
        census=census,
        quote_age_slices=slices,
        outcomes=normalized_outcomes,
        comparisons=comparisons,
        residual_bins=tuple(residual_bins),
        consumption_receipts=tuple(consumption_receipts),
        status=(
            TriangleQualificationStatus.FAIL
            if failures
            else TriangleQualificationStatus.PASS
        ),
        failure_reasons=failures,
        advisories=advisories,
        created_at=created_at,
    )


def write_triangle_alignment_qualification(
    qualification: TriangleAlignmentQualificationV1,
    root: str | Path,
) -> ArtifactRef:
    """Write one immutable content-addressed qualification artifact."""
    payload = qualification.to_json().encode("utf-8")
    return _write_content_addressed(
        payload,
        root,
        prefix="triangle-alignment-qualification",
        kind=TRIANGLE_ALIGNMENT_QUALIFICATION_ARTIFACT_KIND,
        metadata={
            "qualification_id": qualification.qualification_id,
            "candidate_id": qualification.candidate_id,
            "policy_id": qualification.policy.policy_id,
            "maximum_age_ns": qualification.policy.maximum_age_ns,
            "status": qualification.status.value,
        },
    )


def read_triangle_alignment_qualification(
    path: str | Path,
) -> TriangleAlignmentQualificationV1:
    """Read and independently revalidate a qualification artifact."""
    return TriangleAlignmentQualificationV1.from_dict(
        _read_contract(path, "triangle-alignment-qualification")
    )


def _exact_tuples(
    window_id: str,
    events_by_symbol: Mapping[str, Sequence[TriangleAlignmentSourceEventV1]],
) -> tuple[TriangleAlignmentTupleV1, ...]:
    by_time = {
        symbol: _events_grouped_by_time(events_by_symbol[symbol])
        for symbol in TRIANGLE_SYMBOLS
    }
    common = set.intersection(
        *(set(by_time[symbol]) for symbol in TRIANGLE_SYMBOLS)
    )
    result: list[TriangleAlignmentTupleV1] = []
    probe_symbol = TRIANGLE_SYMBOLS[0]
    for event_time in sorted(common):
        count = min(
            len(by_time[symbol][event_time]) for symbol in TRIANGLE_SYMBOLS
        )
        for ordinal in range(count):
            selected = {
                symbol: by_time[symbol][event_time][ordinal]
                for symbol in TRIANGLE_SYMBOLS
            }
            result.append(
                _alignment_tuple(
                    window_id,
                    TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE,
                    0,
                    probe_symbol,
                    selected[probe_symbol],
                    selected,
                )
            )
    return tuple(result)


def _bounded_tuples(
    window_id: str,
    events_by_symbol: Mapping[str, Sequence[TriangleAlignmentSourceEventV1]],
    *,
    probe_symbol: str,
    maximum_age_ns: int,
) -> tuple[TriangleAlignmentTupleV1, ...]:
    ordered = {
        symbol: tuple(sorted(events_by_symbol[symbol], key=_event_order_key))
        for symbol in TRIANGLE_SYMBOLS
    }
    times = {
        symbol: tuple(item.event_time_ns for item in ordered[symbol])
        for symbol in TRIANGLE_SYMBOLS
    }
    result: list[TriangleAlignmentTupleV1] = []
    for probe in ordered[probe_symbol]:
        selected: dict[str, TriangleAlignmentSourceEventV1] = {}
        for symbol in TRIANGLE_SYMBOLS:
            if symbol == probe_symbol:
                selected[symbol] = probe
                continue
            index = bisect_right(times[symbol], probe.event_time_ns) - 1
            if index < 0:
                break
            event = ordered[symbol][index]
            if probe.event_time_ns - event.event_time_ns > maximum_age_ns:
                break
            selected[symbol] = event
        if len(selected) == len(TRIANGLE_SYMBOLS):
            result.append(
                _alignment_tuple(
                    window_id,
                    TriangleAlignmentPolicy.BOUNDED_PRIOR,
                    maximum_age_ns,
                    probe_symbol,
                    probe,
                    selected,
                )
            )
    return tuple(result)


def _alignment_tuple(
    window_id: str,
    policy: TriangleAlignmentPolicy,
    maximum_age_ns: int,
    probe_symbol: str,
    probe: TriangleAlignmentSourceEventV1,
    selected: Mapping[str, TriangleAlignmentSourceEventV1],
) -> TriangleAlignmentTupleV1:
    return TriangleAlignmentTupleV1(
        window_id=window_id,
        policy=policy,
        configured_max_age_ns=maximum_age_ns,
        probe_symbol=probe_symbol,
        probe_event_id=probe.event_id,
        probe_time_ns=probe.event_time_ns,
        selected_event_ids={
            symbol: selected[symbol].event_id for symbol in TRIANGLE_SYMBOLS
        },
        selected_event_times_ns={
            symbol: selected[symbol].event_time_ns
            for symbol in TRIANGLE_SYMBOLS
        },
        selected_event_content_sha256={
            symbol: selected[symbol].source_row_content_sha256
            for symbol in TRIANGLE_SYMBOLS
        },
        ages_ns={
            symbol: probe.event_time_ns - selected[symbol].event_time_ns
            for symbol in TRIANGLE_SYMBOLS
        },
    )


def _events_grouped_by_time(
    events: Sequence[TriangleAlignmentSourceEventV1],
) -> dict[int, tuple[TriangleAlignmentSourceEventV1, ...]]:
    grouped: dict[int, list[TriangleAlignmentSourceEventV1]] = defaultdict(list)
    for item in events:
        grouped[item.event_time_ns].append(item)
    return {
        event_time: tuple(
            sorted(
                values, key=lambda item: (item.event_sequence, item.event_id)
            )
        )
        for event_time, values in grouped.items()
    }


def _validate_support_class(
    evidence: TriangleAlignmentWindowEvidenceV1,
) -> None:
    expected = {
        TriangleSourceWindowState.EMPTY: TriangleSupportClass.EMPTY,
        TriangleSourceWindowState.EXPECTED_CLOSURE: (
            TriangleSupportClass.EXPECTED_CLOSURE
        ),
        TriangleSourceWindowState.INCOMPLETE: (
            TriangleSupportClass.INCOMPLETE_SOURCE
        ),
    }.get(evidence.source_state)
    if expected is not None and evidence.support_class is not expected:
        raise ValueError("alignment support class differs from source state")
    if evidence.support_class is TriangleSupportClass.EXACT and (
        evidence.selected_policy
        is not TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE
    ):
        raise ValueError("exact support class lacks exact selection")
    if evidence.support_class is TriangleSupportClass.BOUNDED_PRIOR_ONLY and (
        evidence.selected_policy is not TriangleAlignmentPolicy.BOUNDED_PRIOR
        or not any(item.bounded_only for item in evidence.selected_tuples)
    ):
        raise ValueError("bounded-only support class lacks stale evidence")
    if (
        evidence.support_class is TriangleSupportClass.UNSUPPORTED_COMPLETE
        and (
            evidence.source_state is not TriangleSourceWindowState.AVAILABLE
            or evidence.selected_policy is not None
        )
    ):
        raise ValueError("unsupported complete class differs from source state")


def _validate_source_window_cover(
    windows: Sequence[TriangleAlignmentSourceWindowV1], candidate_id: str
) -> None:
    if not windows or len(windows) > MAX_ALIGNMENT_WINDOWS:
        raise ValueError("alignment source window count is invalid")
    if any(item.candidate_id != candidate_id for item in windows):
        raise ValueError("alignment source window candidate differs")
    if any(left.end_ns != right.start_ns for left, right in pairwise(windows)):
        raise ValueError("alignment source range has a gap or overlap")


def _validate_evidence_cover(
    windows: Sequence[TriangleAlignmentWindowEvidenceV1], candidate_id: str
) -> None:
    if any(item.candidate_id != candidate_id for item in windows):
        raise ValueError("alignment evidence candidate differs")
    if any(left.end_ns != right.start_ns for left, right in pairwise(windows)):
        raise ValueError("alignment evidence range has a gap or overlap")
    if len({item.source_window_id for item in windows}) != len(windows):
        raise ValueError("alignment evidence repeats source window identity")


def _qualification_findings(
    windows: Sequence[TriangleAlignmentWindowEvidenceV1],
    policy: TriangleAlignmentQualificationPolicyV1,
    outcomes: Sequence[TriangleAlignmentOutcomeV1],
    comparisons: Sequence[TriangleAlignmentComparisonV1],
    residual_bins: Sequence[TriangleAlignmentResidualBinV1],
    receipts: Sequence[TriangleAlignmentConsumptionReceiptV1],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    failures: set[str] = set()
    advisories: set[str] = set()
    evidence_by_window = {item.source_window_id: item for item in windows}
    executable = {
        item.source_window_id: item
        for item in windows
        if item.support_class
        in {TriangleSupportClass.EXACT, TriangleSupportClass.BOUNDED_PRIOR_ONLY}
    }
    outcome_groups: dict[
        tuple[str, str, str, str], list[TriangleAlignmentOutcomeV1]
    ] = defaultdict(list)
    for outcome in outcomes:
        evidence = evidence_by_window.get(outcome.source_window_id)
        if evidence is None:
            failures.add("outcome:unknown_source_window")
            continue
        if (
            outcome.alignment_evidence_id != evidence.evidence_id
            or outcome.source_content_sha256 != evidence.source_content_sha256
        ):
            failures.add(f"{outcome.source_window_id}:outcome_lineage_differs")
        if not outcome.observed_only_residual_immutable:
            failures.add(
                f"{outcome.source_window_id}:observed_residual_mutated"
            )
        if not outcome.synthetic_involved_residual_passed:
            failures.add(
                f"{outcome.source_window_id}:synthetic_residual_failed"
            )
        if outcome.configured_max_age_ns > policy.maximum_age_ns:
            failures.add(
                f"{outcome.source_window_id}:age_ceiling_silently_widened"
            )
        outcome_groups[outcome.treatment_key()].append(outcome)
    for window_id, evidence in executable.items():
        groups = [
            group
            for key, group in outcome_groups.items()
            if key[0] == window_id
        ]
        if evidence.support_class is TriangleSupportClass.EXACT:
            qualified_pairs = 0
            for group in groups:
                treatments = {
                    (item.policy, item.configured_max_age_ns) for item in group
                }
                if {
                    (TriangleAlignmentPolicy.EXACT_EVENT_SEQUENCE, 0),
                    (
                        TriangleAlignmentPolicy.BOUNDED_PRIOR,
                        policy.maximum_age_ns,
                    ),
                }.issubset(treatments):
                    qualified_pairs += 1
            if qualified_pairs < policy.minimum_sensitivity_pairs_per_window:
                failures.add(f"{window_id}:exact_bounded_sensitivity_missing")
        else:
            present_ceilings = {
                item.configured_max_age_ns
                for group in groups
                for item in group
                if item.policy is TriangleAlignmentPolicy.BOUNDED_PRIOR
                and item.validation_only
            }
            missing = set(policy.sensitivity_age_ceilings_ns) - present_ceilings
            if missing:
                failures.add(f"{window_id}:bounded_ceiling_sensitivity_missing")
    for comparison in comparisons:
        for reason in comparison.hard_failures:
            failures.add(f"{comparison.source_window_id}:{reason}")
        for reason in comparison.advisories:
            advisories.add(f"{comparison.source_window_id}:{reason}")
        rule = policy.age_rule(comparison.comparator_max_age_ns)
        if rule.action is TriangleAgeRuleAction.REFUSE:
            failures.add(f"{comparison.source_window_id}:age_region_refused")
        if max(comparison.relative_differences.values(), default=0.0) > (
            rule.maximum_relative_sensitivity
        ):
            failures.add(
                f"{comparison.source_window_id}:age_sensitivity_exceeded"
            )
    selected_ages = {
        item.maximum_age_ns
        for evidence in executable.values()
        for item in evidence.selected_tuples
    } | {item.configured_max_age_ns for item in outcomes}
    bins = tuple(sorted(residual_bins, key=lambda item: item.lower_age_ns))
    for age in selected_ages:
        matching = [
            item
            for item in bins
            if item.lower_age_ns <= age < item.upper_age_ns
        ]
        if len(matching) != 1:
            failures.add(f"age={age}:residual_relation_missing")
            continue
        residual = matching[0]
        rule = policy.age_rule(age)
        if not residual.observed_only_residual_immutable:
            failures.add(f"age={age}:observed_residual_mutated")
        if not residual.synthetic_involved_residual_passed:
            failures.add(f"age={age}:synthetic_residual_failed")
        if rule.action is TriangleAgeRuleAction.REFUSE:
            failures.add(f"age={age}:age_region_refused")
        if (
            residual.synthetic_post_projection_residual_maximum
            > rule.maximum_synthetic_residual
        ):
            failures.add(f"age={age}:synthetic_residual_tolerance_exceeded")
        if residual.projection_burden_maximum > rule.maximum_projection_burden:
            failures.add(f"age={age}:projection_burden_tolerance_exceeded")
    receipts_by_window = {item.source_window_id: item for item in receipts}
    for window_id, evidence in executable.items():
        receipt = receipts_by_window.get(window_id)
        if receipt is None:
            failures.add(f"{window_id}:runtime_consumption_receipt_missing")
        elif (
            not receipt.matched
            or receipt.planner_alignment_evidence_id != evidence.evidence_id
            or receipt.planner_tuple_content_sha256
            != evidence.selected_tuple_content_sha256
            or receipt.expected_policy_id != policy.policy_id
        ):
            failures.add(f"{window_id}:runtime_consumption_differs")
    if set(receipts_by_window) - set(executable):
        failures.add("runtime_receipt:non_executable_window")
    return tuple(sorted(failures)), tuple(sorted(advisories))


def _age_quantiles(ages: Sequence[int]) -> dict[str, int]:
    if not ages:
        raise ValueError("quote-age quantiles require samples")
    ordered = tuple(sorted(ages))

    def value(probability: float) -> int:
        return ordered[max(0, math.ceil(len(ordered) * probability) - 1)]

    return {
        "p0": ordered[0],
        "p50": value(0.50),
        "p90": value(0.90),
        "p95": value(0.95),
        "p99": value(0.99),
        "p100": ordered[-1],
    }


def _quantile_mapping(value: Mapping[str, int]) -> dict[str, int]:
    expected = ("p0", "p50", "p90", "p95", "p99", "p100")
    if set(value) != set(expected):
        raise ValueError("quote-age quantiles are incomplete")
    result = {name: _nonnegative_int(value[name], name) for name in expected}
    if tuple(result.values()) != tuple(sorted(result.values())):
        raise ValueError("quote-age quantiles are not monotonic")
    return result


def _event_order_key(
    event: TriangleAlignmentSourceEventV1,
) -> tuple[str, int, int, str]:
    return (
        event.symbol,
        event.event_time_ns,
        event.event_sequence,
        event.event_id,
    )


def _verify_release_candidate_ref(ref: ArtifactRef, candidate_id: str) -> None:
    path = Path(ref.path).expanduser()
    payload = path.read_bytes()
    if (
        len(payload) != ref.size_bytes
        or hashlib.sha256(payload).hexdigest() != ref.sha256
    ):
        raise ValueError("release candidate artifact bytes differ")
    if ref.metadata.get("candidate_id") != candidate_id:
        raise ValueError("release candidate artifact identity differs")


def _write_content_addressed(
    payload: bytes,
    root: str | Path,
    *,
    prefix: str,
    kind: str,
    metadata: Mapping[str, JSONValue],
) -> ArtifactRef:
    if len(payload) > MAX_ALIGNMENT_ARTIFACT_BYTES:
        raise ValueError("alignment qualification artifact exceeds size limit")
    destination = Path(root).expanduser()
    destination.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(payload).hexdigest()
    path = destination / f"{prefix}-{digest}.json"
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError("content-addressed alignment artifact differs")
    else:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
    return ArtifactRef(
        kind=kind,
        path=str(path),
        size_bytes=len(payload),
        sha256=digest,
        metadata=dict(metadata),
    )


def _read_contract(path: str | Path, prefix: str) -> Mapping[str, Any]:
    target = Path(path).expanduser()
    payload = target.read_bytes()
    if len(payload) > MAX_ALIGNMENT_ARTIFACT_BYTES:
        raise ValueError("alignment qualification artifact exceeds size limit")
    digest = hashlib.sha256(payload).hexdigest()
    if target.name != f"{prefix}-{digest}.json":
        raise ValueError(
            "alignment qualification artifact is not content addressed"
        )
    return _mapping(json.loads(payload))


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    return f"{prefix}:sha256:{_content_sha256(payload)}"


def _content_sha256(
    value: JSONValue | Mapping[str, JSONValue],
) -> str:
    return hashlib.sha256(
        canonical_contract_json(value).encode("utf-8")
    ).hexdigest()


def _require_schema(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported alignment schema: {actual!r}")


def _required_text(value: Any) -> str:
    normalized = str(value).strip() if value is not None else ""
    if not normalized or len(normalized) > MAX_ALIGNMENT_TEXT:
        raise ValueError("required alignment text is invalid")
    return normalized


def _text_tuple(values: Sequence[str]) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item) for item in values}))
    if len(result) > MAX_ALIGNMENT_WINDOWS:
        raise ValueError("alignment text collection exceeds bound")
    return result


def _symbol(value: Any) -> str:
    return _required_text(value).lower().replace("/", "")


def _strict_int(value: Any, name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    return value


def _int64(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if not -(2**63) <= result < 2**63:
        raise ValueError(f"{name} lies outside int64")
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


def _finite_float(value: Any, name: str) -> float:
    if type(value) not in (int, float):
        raise TypeError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _positive_finite(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _nonnegative_finite(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if result < 0:
        raise ValueError(f"{name} must be nonnegative")
    return result


def _unit_float(value: Any, name: str) -> float:
    result = _finite_float(value, name)
    if not 0 <= result <= 1:
        raise ValueError(f"{name} must lie in [0, 1]")
    return result


def _sha256(value: Any, name: str) -> str:
    normalized = str(value).strip().lower()
    if not _SHA256.fullmatch(normalized):
        raise ValueError(f"{name} is not SHA-256")
    return normalized


def _strict_bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{name} must be boolean")
    return value


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("alignment value must be a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("alignment value must be a sequence")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _text_mapping(value: Any) -> dict[str, str]:
    return {str(key): str(item) for key, item in _mapping(value).items()}


def _int_mapping(value: Any) -> dict[str, int]:
    return {
        str(key): _strict_int(item, str(key))
        for key, item in _mapping(value).items()
    }


def _float_mapping(value: Any) -> dict[str, float]:
    return {
        str(key): _finite_float(item, str(key))
        for key, item in _mapping(value).items()
    }


def _triangle_text_mapping(
    value: Mapping[str, str], name: str
) -> dict[str, str]:
    if set(value) != set(TRIANGLE_SYMBOLS):
        raise ValueError(f"{name} must cover the exact triangle")
    return {
        symbol: _required_text(value[symbol]) for symbol in TRIANGLE_SYMBOLS
    }


def _triangle_int_mapping(
    value: Mapping[str, int], name: str
) -> dict[str, int]:
    if set(value) != set(TRIANGLE_SYMBOLS):
        raise ValueError(f"{name} must cover the exact triangle")
    return {
        symbol: _strict_int(value[symbol], f"{name}.{symbol}")
        for symbol in TRIANGLE_SYMBOLS
    }


def _triangle_sha_mapping(
    value: Mapping[str, str], name: str
) -> dict[str, str]:
    if set(value) != set(TRIANGLE_SYMBOLS):
        raise ValueError(f"{name} must cover the exact triangle")
    return {
        symbol: _sha256(value[symbol], f"{name}.{symbol}")
        for symbol in TRIANGLE_SYMBOLS
    }


def _nonnegative_named_mapping(
    value: Mapping[str, int], expected_keys: set[str]
) -> dict[str, int]:
    if set(value) != expected_keys:
        raise ValueError("alignment census categories are incomplete")
    return {
        key: _nonnegative_int(value[key], key) for key in sorted(expected_keys)
    }


def _metric_mapping(
    value: Mapping[str, float], required: frozenset[str]
) -> dict[str, float]:
    if set(value) != set(required):
        raise ValueError("alignment metrics are incomplete")
    return {key: _finite_float(value[key], key) for key in sorted(required)}


def _timestamp(value: str) -> str:
    normalized = _required_text(value)
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as err:
        raise ValueError(
            "alignment qualification timestamp is invalid"
        ) from err
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("alignment qualification timestamp needs timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
