"""Historical as-known economic-release schedule reconstruction.

This module binds expected catalog occurrences to the immutable release chains
from :mod:`release_vintages`.  It deliberately queries scheduled time rather
than publication time, so a later reschedule changes the historical surface
only when that schedule evidence became public and an actual publication never
retroactively changes what had been scheduled.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from itertools import pairwise
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarReleaseV1,
    EconomicEventFamily,
    EconomicReleaseStatus,
)
from histdatacom.market_context.indicator_catalog import (
    EconomicExpectedOccurrenceStatus,
    EconomicExpectedOccurrenceV1,
    EconomicIndicatorCatalogV1,
)
from histdatacom.market_context.release_vintages import (
    EconomicReleaseVintageChainV1,
)
from histdatacom.runtime_contracts import JSONValue

ECONOMIC_RELEASE_SCHEDULE_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.economic-release-schedule-evidence.v1"
)
ECONOMIC_RELEASE_SCHEDULE_CHAIN_SCHEMA_VERSION = (
    "histdatacom.economic-release-schedule-chain.v1"
)
ECONOMIC_RELEASE_SCHEDULE_CORPUS_SCHEMA_VERSION = (
    "histdatacom.economic-release-schedule-corpus.v1"
)
ECONOMIC_RELEASE_SCHEDULE_STATE_SCHEMA_VERSION = (
    "histdatacom.economic-release-schedule-state.v1"
)
ECONOMIC_RELEASE_SCHEDULE_QUERY_SCHEMA_VERSION = (
    "histdatacom.economic-release-schedule-query.v1"
)
ECONOMIC_RELEASE_SCHEDULE_AUDIT_SCHEMA_VERSION = (
    "histdatacom.economic-release-schedule-audit.v1"
)

MAX_ECONOMIC_RELEASE_SCHEDULE_CHAINS = 100_000
MAX_ECONOMIC_RELEASE_SCHEDULE_QUERY_EVENTS = 2_048
MAX_ECONOMIC_RELEASE_SCHEDULE_CORPUS_BYTES = 64 * 1024 * 1024

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class EconomicScheduleExceptionKind(str, Enum):
    """Why a schedule vintage differs from an ordinary recurrence."""

    ROUTINE = "routine"
    HOLIDAY_SHIFT = "holiday-shift"
    DST_TRANSITION = "dst-transition"
    RESCHEDULE = "reschedule"
    PRODUCER_DELAY = "producer-delay"
    CANCELLATION = "cancellation"
    UNSCHEDULED_EMERGENCY = "unscheduled-emergency"
    SOURCE_CALENDAR_OUTAGE = "source-calendar-outage"

    @classmethod
    def from_value(
        cls, value: str | EconomicScheduleExceptionKind
    ) -> EconomicScheduleExceptionKind:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported schedule exception kind") from exc


def _required_text(value: object, name: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise ValueError(f"{name} is required")
    return result


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _key(value: object, name: str) -> str:
    result = _required_text(value, name).lower()
    if _KEY_RE.fullmatch(result) is None:
        raise ValueError(f"{name} is not a canonical key")
    return result


def _bounded_ns(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= 2**63 - 1:
        raise ValueError(f"{name} is outside the supported range")
    return value


def _bounded_count(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside the supported range")
    return value


def _texts(
    values: Iterable[object], name: str, *, required: bool = False
) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item, name) for item in values}))
    if required and not result:
        raise ValueError(f"{name} must not be empty")
    if len(result) > 256:
        raise ValueError(f"{name} exceeds the item bound")
    return result


def _ordered_texts(
    values: Iterable[object], name: str, *, required: bool = False
) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        text = _required_text(value, name)
        if text in result:
            raise ValueError(f"{name} contains a duplicate")
        result.append(text)
    if required and not result:
        raise ValueError(f"{name} must not be empty")
    if len(result) > 10_001:
        raise ValueError(f"{name} exceeds the item bound")
    return tuple(result)


def _mapping(value: object, name: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _sequence(value: object, name: str = "sequence") -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


@dataclass(frozen=True, slots=True)
class EconomicReleaseScheduleEvidenceV1:
    """Calendar, holiday, and timezone evidence for one release vintage."""

    release_id: str
    source_key: str
    known_at_ns: int
    timezone_database_version: str
    holiday_calendar_version: str
    dst_fold: int | None
    exception_kind: EconomicScheduleExceptionKind
    exception_reason: str | None
    evidence_sha256: str
    evidence_id: str = ""
    schema_version: str = ECONOMIC_RELEASE_SCHEDULE_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_RELEASE_SCHEDULE_EVIDENCE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported release schedule-evidence schema")
        object.__setattr__(
            self, "release_id", _required_text(self.release_id, "release_id")
        )
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        object.__setattr__(
            self, "known_at_ns", _bounded_ns(self.known_at_ns, "known_at_ns")
        )
        for name in ("timezone_database_version", "holiday_calendar_version"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        if self.dst_fold not in {None, 0, 1}:
            raise ValueError("dst_fold must be zero, one, or unavailable")
        kind = EconomicScheduleExceptionKind.from_value(self.exception_kind)
        object.__setattr__(self, "exception_kind", kind)
        reason = _optional_text(self.exception_reason)
        if kind is EconomicScheduleExceptionKind.ROUTINE:
            if reason is not None:
                raise ValueError(
                    "routine schedule evidence cannot claim an exception"
                )
        elif reason is None:
            raise ValueError("schedule exception evidence requires a reason")
        object.__setattr__(self, "exception_reason", reason)
        digest = _required_text(self.evidence_sha256, "evidence_sha256")
        if _SHA256_RE.fullmatch(digest) is None:
            raise ValueError(
                "evidence_sha256 must be a lowercase SHA-256 digest"
            )
        object.__setattr__(self, "evidence_sha256", digest)
        expected = _stable_id(
            "economic-release-schedule-evidence", self.identity_payload()
        )
        supplied = _optional_text(self.evidence_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "evidence_id does not match deterministic identity"
            )
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "release_id": self.release_id,
            "source_key": self.source_key,
            "known_at_ns": self.known_at_ns,
            "timezone_database_version": self.timezone_database_version,
            "holiday_calendar_version": self.holiday_calendar_version,
            "dst_fold": self.dst_fold,
            "exception_kind": self.exception_kind.value,
            "exception_reason": self.exception_reason,
            "evidence_sha256": self.evidence_sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicReleaseScheduleEvidenceV1:
        return cls(
            release_id=str(data.get("release_id", "")),
            source_key=str(data.get("source_key", "")),
            known_at_ns=cast(int, data.get("known_at_ns")),
            timezone_database_version=str(
                data.get("timezone_database_version", "")
            ),
            holiday_calendar_version=str(
                data.get("holiday_calendar_version", "")
            ),
            dst_fold=cast(int | None, data.get("dst_fold")),
            exception_kind=EconomicScheduleExceptionKind.from_value(
                str(data.get("exception_kind", ""))
            ),
            exception_reason=_optional_text(data.get("exception_reason")),
            evidence_sha256=str(data.get("evidence_sha256", "")),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicReleaseScheduleChainV1:
    """Catalog-bound schedule evidence over one exact vintage chain."""

    vintage_chain: EconomicReleaseVintageChainV1
    expected_occurrence: EconomicExpectedOccurrenceV1 | None
    schedule_evidence: tuple[EconomicReleaseScheduleEvidenceV1, ...]
    limitations: tuple[str, ...]
    chain_id: str = ""
    schema_version: str = ECONOMIC_RELEASE_SCHEDULE_CHAIN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_RELEASE_SCHEDULE_CHAIN_SCHEMA_VERSION
        ):
            raise ValueError("unsupported release schedule-chain schema")
        if not isinstance(self.vintage_chain, EconomicReleaseVintageChainV1):
            raise TypeError(
                "vintage_chain must use the v1 reconstruction contract"
            )
        occurrence = self.expected_occurrence
        initial = self.vintage_chain.initial_release
        if occurrence is None:
            if initial.status is not EconomicReleaseStatus.UNSCHEDULED:
                raise ValueError(
                    "only an unscheduled event may omit an expected occurrence"
                )
        else:
            if not isinstance(occurrence, EconomicExpectedOccurrenceV1):
                raise TypeError("expected_occurrence must use the v1 contract")
            if (
                occurrence.logical_event_key != initial.logical_event_key
                or occurrence.indicator_key != initial.series_key
            ):
                raise ValueError(
                    "expected occurrence differs from release identity"
                )
            if occurrence.expected_for_ns != initial.scheduled_for_ns:
                raise ValueError(
                    "expected occurrence differs from initial schedule"
                )
        evidence = tuple(self.schedule_evidence)
        releases = self.vintage_chain.releases
        if len(evidence) != len(releases) or any(
            not isinstance(item, EconomicReleaseScheduleEvidenceV1)
            for item in evidence
        ):
            raise ValueError(
                "schedule evidence must cover every release vintage"
            )
        if tuple(item.release_id for item in evidence) != tuple(
            item.release_id for item in releases
        ):
            raise ValueError(
                "schedule evidence order differs from release replay"
            )
        source_keys = {item.source_key for item in evidence}
        if len(source_keys) != 1:
            raise ValueError("schedule evidence changes official source")
        if occurrence is not None and source_keys != {occurrence.source_key}:
            raise ValueError("schedule evidence differs from occurrence source")
        if (
            occurrence is not None
            and occurrence.status_known_at_ns > evidence[-1].known_at_ns
        ):
            raise ValueError("occurrence status uses future schedule evidence")
        for release, item in zip(releases, evidence, strict=True):
            if item.known_at_ns != release.available_at_ns:
                raise ValueError(
                    "schedule evidence knowledge time differs from release"
                )
            _validate_schedule_exception(release, item, occurrence is None)
        for previous, current in pairwise(releases):
            changed = previous.scheduled_for_ns != current.scheduled_for_ns
            if changed and current.status not in {
                EconomicReleaseStatus.RESCHEDULED,
                EconomicReleaseStatus.DELAYED,
            }:
                raise ValueError(
                    "schedule time changes without reschedule status"
                )
            if (
                not changed
                and current.status is EconomicReleaseStatus.RESCHEDULED
            ):
                raise ValueError("reschedule does not change scheduled time")
        object.__setattr__(self, "schedule_evidence", evidence)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id(
            "economic-release-schedule-chain", self.identity_payload()
        )
        supplied = _optional_text(self.chain_id)
        if supplied is not None and supplied != expected:
            raise ValueError("chain_id does not match deterministic identity")
        object.__setattr__(self, "chain_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "vintage_chain": self.vintage_chain.to_dict(),
            "expected_occurrence": (
                None
                if self.expected_occurrence is None
                else self.expected_occurrence.to_dict()
            ),
            "schedule_evidence": [
                item.to_dict() for item in self.schedule_evidence
            ],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "chain_id": self.chain_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicReleaseScheduleChainV1:
        raw_occurrence = data.get("expected_occurrence")
        return cls(
            vintage_chain=EconomicReleaseVintageChainV1.from_dict(
                _mapping(data.get("vintage_chain"))
            ),
            expected_occurrence=(
                None
                if raw_occurrence is None
                else EconomicExpectedOccurrenceV1.from_dict(
                    _mapping(raw_occurrence)
                )
            ),
            schedule_evidence=tuple(
                EconomicReleaseScheduleEvidenceV1.from_dict(_mapping(item))
                for item in _sequence(data.get("schedule_evidence"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            chain_id=str(data.get("chain_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicReleaseScheduleCorpusV1:
    """Bounded schedule chains tied to one immutable indicator catalog."""

    catalog_id: str
    coverage_start_ns: int
    coverage_end_ns: int
    complete: bool
    chains: tuple[EconomicReleaseScheduleChainV1, ...]
    limitations: tuple[str, ...]
    corpus_id: str = ""
    schema_version: str = ECONOMIC_RELEASE_SCHEDULE_CORPUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_RELEASE_SCHEDULE_CORPUS_SCHEMA_VERSION
        ):
            raise ValueError("unsupported release schedule-corpus schema")
        catalog_id = _required_text(self.catalog_id, "catalog_id")
        if not catalog_id.startswith("economic-indicator-catalog:sha256:"):
            raise ValueError("catalog_id is not an indicator catalog identity")
        object.__setattr__(self, "catalog_id", catalog_id)
        start = _bounded_ns(self.coverage_start_ns, "coverage_start_ns")
        end = _bounded_ns(self.coverage_end_ns, "coverage_end_ns")
        if start >= end:
            raise ValueError("schedule corpus interval must be nonempty")
        object.__setattr__(self, "coverage_start_ns", start)
        object.__setattr__(self, "coverage_end_ns", end)
        if not isinstance(self.complete, bool):
            raise TypeError("complete must be boolean")
        raw_chains = tuple(self.chains)
        if any(
            not isinstance(item, EconomicReleaseScheduleChainV1)
            for item in raw_chains
        ):
            raise TypeError("schedule corpus requires v1 schedule chains")
        chains = tuple(
            sorted(
                raw_chains,
                key=lambda item: item.vintage_chain.initial_release.logical_event_key,
            )
        )
        if len(chains) > MAX_ECONOMIC_RELEASE_SCHEDULE_CHAINS:
            raise ValueError("schedule chain count is outside v1 bounds")
        event_keys = tuple(
            item.vintage_chain.initial_release.logical_event_key
            for item in chains
        )
        if len(set(event_keys)) != len(event_keys):
            raise ValueError("schedule corpus repeats a logical event")
        occurrence_ids = tuple(
            item.expected_occurrence.occurrence_id
            for item in chains
            if item.expected_occurrence is not None
        )
        if len(set(occurrence_ids)) != len(occurrence_ids):
            raise ValueError("schedule corpus repeats an expected occurrence")
        if any(
            not start <= release.scheduled_for_ns < end
            for chain in chains
            for release in chain.vintage_chain.releases
        ):
            raise ValueError("schedule vintage lies outside corpus coverage")
        object.__setattr__(self, "chains", chains)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id(
            "economic-release-schedule-corpus", self.identity_payload()
        )
        supplied = _optional_text(self.corpus_id)
        if supplied is not None and supplied != expected:
            raise ValueError("corpus_id does not match deterministic identity")
        object.__setattr__(self, "corpus_id", expected)
        if (
            len(self.to_json().encode("utf-8"))
            > MAX_ECONOMIC_RELEASE_SCHEDULE_CORPUS_BYTES
        ):
            raise ValueError("release schedule corpus exceeds byte bound")

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "catalog_id": self.catalog_id,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "complete": self.complete,
            "chains": [item.to_dict() for item in self.chains],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "corpus_id": self.corpus_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicReleaseScheduleCorpusV1:
        return cls(
            catalog_id=str(data.get("catalog_id", "")),
            coverage_start_ns=cast(int, data.get("coverage_start_ns")),
            coverage_end_ns=cast(int, data.get("coverage_end_ns")),
            complete=cast(bool, data.get("complete")),
            chains=tuple(
                EconomicReleaseScheduleChainV1.from_dict(_mapping(item))
                for item in _sequence(data.get("chains"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            corpus_id=str(data.get("corpus_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicReleaseScheduleCorpusV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic release schedule corpus is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicReleaseScheduleStateV1:
    """Latest schedule vintage admissible at one historical decision time."""

    chain_id: str
    expected_occurrence_id: str | None
    decision_at_ns: int
    visible_release_ids: tuple[str, ...]
    release: EconomicCalendarReleaseV1
    evidence: EconomicReleaseScheduleEvidenceV1
    state_id: str = ""
    schema_version: str = ECONOMIC_RELEASE_SCHEDULE_STATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_RELEASE_SCHEDULE_STATE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported release schedule-state schema")
        chain_id = _required_text(self.chain_id, "chain_id")
        if not chain_id.startswith("economic-release-schedule-chain:sha256:"):
            raise ValueError(
                "chain_id is not a release schedule-chain identity"
            )
        object.__setattr__(self, "chain_id", chain_id)
        occurrence_id = _optional_text(self.expected_occurrence_id)
        if occurrence_id is not None and not occurrence_id.startswith(
            "economic-expected-occurrence:sha256:"
        ):
            raise ValueError(
                "expected_occurrence_id is not an expected-occurrence identity"
            )
        object.__setattr__(self, "expected_occurrence_id", occurrence_id)
        decision = _bounded_ns(self.decision_at_ns, "decision_at_ns")
        object.__setattr__(self, "decision_at_ns", decision)
        if not isinstance(
            self.release, EconomicCalendarReleaseV1
        ) or not isinstance(self.evidence, EconomicReleaseScheduleEvidenceV1):
            raise TypeError("schedule state requires v1 release and evidence")
        visible = _ordered_texts(
            self.visible_release_ids, "visible_release_ids", required=True
        )
        if (
            self.release.release_id != visible[-1]
            or self.evidence.release_id != self.release.release_id
        ):
            raise ValueError("schedule state is not the latest visible release")
        if self.evidence.known_at_ns != self.release.available_at_ns:
            raise ValueError(
                "schedule state knowledge time differs from release"
            )
        if (
            self.release.available_at_ns > decision
            or self.evidence.known_at_ns > decision
        ):
            raise ValueError("schedule state contains future evidence")
        object.__setattr__(self, "visible_release_ids", visible)
        expected = _stable_id(
            "economic-release-schedule-state", self.identity_payload()
        )
        supplied = _optional_text(self.state_id)
        if supplied is not None and supplied != expected:
            raise ValueError("state_id does not match deterministic identity")
        object.__setattr__(self, "state_id", expected)

    @property
    def scheduled_for_ns(self) -> int:
        return int(self.release.scheduled_for_ns)

    @property
    def known_at_ns(self) -> int:
        return self.evidence.known_at_ns

    @property
    def advance_notice_ns(self) -> int:
        return self.scheduled_for_ns - self.known_at_ns

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "chain_id": self.chain_id,
            "expected_occurrence_id": self.expected_occurrence_id,
            "decision_at_ns": self.decision_at_ns,
            "visible_release_ids": list(self.visible_release_ids),
            "release": self.release.to_dict(),
            "evidence": self.evidence.to_dict(),
            "time_surface": "scheduled_for_ns",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "state_id": self.state_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicReleaseScheduleStateV1:
        return cls(
            chain_id=str(data.get("chain_id", "")),
            expected_occurrence_id=_optional_text(
                data.get("expected_occurrence_id")
            ),
            decision_at_ns=cast(int, data.get("decision_at_ns")),
            visible_release_ids=tuple(
                str(item) for item in _sequence(data.get("visible_release_ids"))
            ),
            release=EconomicCalendarReleaseV1.from_dict(
                _mapping(data.get("release"))
            ),
            evidence=EconomicReleaseScheduleEvidenceV1.from_dict(
                _mapping(data.get("evidence"))
            ),
            state_id=str(data.get("state_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EconomicReleaseScheduleQueryV1:
    """Bounded historical answer to what was believed scheduled."""

    corpus_id: str
    start_ns: int
    end_ns: int
    decision_at_ns: int
    states: tuple[EconomicReleaseScheduleStateV1, ...]
    limitations: tuple[str, ...]
    query_id: str = ""
    schema_version: str = ECONOMIC_RELEASE_SCHEDULE_QUERY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_RELEASE_SCHEDULE_QUERY_SCHEMA_VERSION
        ):
            raise ValueError("unsupported release schedule-query schema")
        corpus_id = _required_text(self.corpus_id, "corpus_id")
        if not corpus_id.startswith("economic-release-schedule-corpus:sha256:"):
            raise ValueError(
                "corpus_id is not a release schedule-corpus identity"
            )
        object.__setattr__(self, "corpus_id", corpus_id)
        start = _bounded_ns(self.start_ns, "start_ns")
        end = _bounded_ns(self.end_ns, "end_ns")
        decision = _bounded_ns(self.decision_at_ns, "decision_at_ns")
        if start >= end:
            raise ValueError("schedule query interval must be nonempty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        object.__setattr__(self, "decision_at_ns", decision)
        raw_states = tuple(self.states)
        if any(
            not isinstance(item, EconomicReleaseScheduleStateV1)
            for item in raw_states
        ):
            raise TypeError("schedule query requires v1 schedule states")
        states = tuple(
            sorted(
                raw_states,
                key=lambda item: (
                    item.scheduled_for_ns,
                    item.release.logical_event_key,
                ),
            )
        )
        if len(states) > MAX_ECONOMIC_RELEASE_SCHEDULE_QUERY_EVENTS or any(
            item.decision_at_ns != decision for item in states
        ):
            raise ValueError("schedule query states are outside v1 bounds")
        if len({item.chain_id for item in states}) != len(states) or len(
            {item.release.logical_event_key for item in states}
        ) != len(states):
            raise ValueError("schedule query repeats an event state")
        if any(not start <= item.scheduled_for_ns < end for item in states):
            raise ValueError("schedule state lies outside requested interval")
        object.__setattr__(self, "states", states)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id(
            "economic-release-schedule-query", self.identity_payload()
        )
        supplied = _optional_text(self.query_id)
        if supplied is not None and supplied != expected:
            raise ValueError("query_id does not match deterministic identity")
        object.__setattr__(self, "query_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "corpus_id": self.corpus_id,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "decision_at_ns": self.decision_at_ns,
            "admission_rule": "schedule_evidence.known_at_ns <= decision_at_ns",
            "time_surface": "scheduled_for_ns",
            "states": [item.to_dict() for item in self.states],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "query_id": self.query_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicReleaseScheduleQueryV1:
        return cls(
            corpus_id=str(data.get("corpus_id", "")),
            start_ns=cast(int, data.get("start_ns")),
            end_ns=cast(int, data.get("end_ns")),
            decision_at_ns=cast(int, data.get("decision_at_ns")),
            states=tuple(
                EconomicReleaseScheduleStateV1.from_dict(_mapping(item))
                for item in _sequence(data.get("states"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            query_id=str(data.get("query_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicReleaseScheduleQueryV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic release schedule query is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


@dataclass(frozen=True, slots=True)
class EconomicReleaseScheduleAuditV1:
    """Machine checks over no-future schedule replay and difficult cases."""

    corpus_id: str
    catalog_id: str
    chain_count: int
    schedule_vintage_count: int
    exception_counts: tuple[tuple[str, int], ...]
    rescheduled_event_keys: tuple[str, ...]
    cancelled_event_keys: tuple[str, ...]
    unscheduled_event_keys: tuple[str, ...]
    occurrence_status_mismatches: tuple[str, ...]
    no_future_violations: tuple[str, ...]
    audit_id: str = ""
    schema_version: str = ECONOMIC_RELEASE_SCHEDULE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_RELEASE_SCHEDULE_AUDIT_SCHEMA_VERSION
        ):
            raise ValueError("unsupported release schedule-audit schema")
        for name in ("corpus_id", "catalog_id"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        _bounded_count(
            self.chain_count,
            "chain_count",
            MAX_ECONOMIC_RELEASE_SCHEDULE_CHAINS,
        )
        _bounded_count(
            self.schedule_vintage_count, "schedule_vintage_count", 10_000_000
        )
        counts = tuple(
            sorted(
                (str(key), int(value)) for key, value in self.exception_counts
            )
        )
        if (
            len({key for key, _ in counts}) != len(counts)
            or any(value < 0 for _, value in counts)
            or sum(value for _, value in counts) != self.schedule_vintage_count
        ):
            raise ValueError("schedule audit exception counts are inconsistent")
        if any(
            key not in {item.value for item in EconomicScheduleExceptionKind}
            for key, _ in counts
        ):
            raise ValueError("schedule audit contains an unknown exception")
        object.__setattr__(self, "exception_counts", counts)
        for name in (
            "rescheduled_event_keys",
            "cancelled_event_keys",
            "unscheduled_event_keys",
            "occurrence_status_mismatches",
            "no_future_violations",
        ):
            object.__setattr__(self, name, _texts(getattr(self, name), name))
        expected = _stable_id(
            "economic-release-schedule-audit", self.identity_payload()
        )
        supplied = _optional_text(self.audit_id)
        if supplied is not None and supplied != expected:
            raise ValueError("audit_id does not match deterministic identity")
        object.__setattr__(self, "audit_id", expected)

    @property
    def passed(self) -> bool:
        return (
            not self.occurrence_status_mismatches
            and not self.no_future_violations
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "corpus_id": self.corpus_id,
            "catalog_id": self.catalog_id,
            "chain_count": self.chain_count,
            "schedule_vintage_count": self.schedule_vintage_count,
            "exception_counts": [
                {"kind": key, "count": value}
                for key, value in self.exception_counts
            ],
            "rescheduled_event_keys": list(self.rescheduled_event_keys),
            "cancelled_event_keys": list(self.cancelled_event_keys),
            "unscheduled_event_keys": list(self.unscheduled_event_keys),
            "occurrence_status_mismatches": list(
                self.occurrence_status_mismatches
            ),
            "no_future_violations": list(self.no_future_violations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "audit_id": self.audit_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicReleaseScheduleAuditV1:
        counts = tuple(
            (
                str(_mapping(item, "exception count").get("kind", "")),
                cast(int, _mapping(item, "exception count").get("count")),
            )
            for item in _sequence(data.get("exception_counts"))
        )
        return cls(
            corpus_id=str(data.get("corpus_id", "")),
            catalog_id=str(data.get("catalog_id", "")),
            chain_count=cast(int, data.get("chain_count")),
            schedule_vintage_count=cast(
                int, data.get("schedule_vintage_count")
            ),
            exception_counts=counts,
            rescheduled_event_keys=tuple(
                str(item)
                for item in _sequence(data.get("rescheduled_event_keys"))
            ),
            cancelled_event_keys=tuple(
                str(item)
                for item in _sequence(data.get("cancelled_event_keys"))
            ),
            unscheduled_event_keys=tuple(
                str(item)
                for item in _sequence(data.get("unscheduled_event_keys"))
            ),
            occurrence_status_mismatches=tuple(
                str(item)
                for item in _sequence(data.get("occurrence_status_mismatches"))
            ),
            no_future_violations=tuple(
                str(item)
                for item in _sequence(data.get("no_future_violations"))
            ),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicReleaseScheduleAuditV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "economic release schedule audit is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload))


def build_economic_release_schedule_corpus(
    catalog: EconomicIndicatorCatalogV1,
    chains: Sequence[EconomicReleaseScheduleChainV1],
    *,
    coverage_start_ns: int,
    coverage_end_ns: int,
    complete: bool,
    limitations: Sequence[str],
) -> EconomicReleaseScheduleCorpusV1:
    """Bind reconstructed schedule chains to exact catalog definitions."""
    if not isinstance(catalog, EconomicIndicatorCatalogV1):
        raise TypeError("schedule corpus requires a v1 indicator catalog")
    for chain in chains:
        if not isinstance(chain, EconomicReleaseScheduleChainV1):
            raise TypeError("schedule corpus requires v1 schedule chains")
        initial = chain.vintage_chain.initial_release
        entry = catalog.entry(initial.series_key)
        if (
            initial.economy_code != entry.economy_code
            or initial.event_family != entry.event_family
            or initial.indicator_id != entry.indicator_id
        ):
            raise ValueError("schedule chain differs from catalog identity")
        occurrence = chain.expected_occurrence
        if occurrence is not None:
            matching_rules = tuple(
                rule
                for rule in entry.expected_release_rules
                if rule.rule_id == occurrence.rule_id
            )
            if len(matching_rules) != 1:
                raise ValueError(
                    "schedule occurrence uses an unknown catalog rule"
                )
            if not matching_rules[0].qualified:
                raise ValueError(
                    "schedule occurrence rule is not evidence-qualified"
                )
            rule = matching_rules[0]
            occurrence_date = (
                datetime.fromtimestamp(
                    occurrence.expected_for_ns // 1_000_000_000,
                    tz=timezone.utc,
                )
                .astimezone(ZoneInfo(rule.source_timezone))
                .date()
                .isoformat()
            )
            if occurrence_date < rule.effective_from or (
                rule.effective_to is not None
                and occurrence_date > rule.effective_to
            ):
                raise ValueError(
                    "schedule occurrence lies outside its catalog rule"
                )
        if {item.source_key for item in chain.schedule_evidence} != {
            entry.legal_producer_source_key
        }:
            raise ValueError("schedule evidence changes the legal producer")
    return EconomicReleaseScheduleCorpusV1(
        catalog_id=catalog.catalog_id,
        coverage_start_ns=coverage_start_ns,
        coverage_end_ns=coverage_end_ns,
        complete=complete,
        chains=tuple(chains),
        limitations=tuple(limitations),
    )


def query_economic_release_schedule_as_known(
    corpus: EconomicReleaseScheduleCorpusV1,
    *,
    start_ns: int,
    end_ns: int,
    decision_at_ns: int,
    event_families: Sequence[str] = (),
    include_cancelled: bool = True,
    max_events: int = MAX_ECONOMIC_RELEASE_SCHEDULE_QUERY_EVENTS,
) -> EconomicReleaseScheduleQueryV1:
    """Return only schedule vintages publicly known by a historical cutoff."""
    if not isinstance(corpus, EconomicReleaseScheduleCorpusV1):
        raise TypeError("schedule query requires a v1 schedule corpus")
    start = _bounded_ns(start_ns, "start_ns")
    end = _bounded_ns(end_ns, "end_ns")
    decision = _bounded_ns(decision_at_ns, "decision_at_ns")
    if (
        start >= end
        or start < corpus.coverage_start_ns
        or end > corpus.coverage_end_ns
    ):
        raise ValueError("schedule query interval lies outside corpus coverage")
    maximum = _bounded_count(
        max_events, "max_events", MAX_ECONOMIC_RELEASE_SCHEDULE_QUERY_EVENTS
    )
    if maximum == 0:
        raise ValueError("max_events must be positive")
    families = {
        EconomicEventFamily.from_value(item).value for item in event_families
    }
    if not isinstance(include_cancelled, bool):
        raise TypeError("include_cancelled must be boolean")
    states: list[EconomicReleaseScheduleStateV1] = []
    for chain in corpus.chains:
        visible_pairs = [
            (release, evidence)
            for release, evidence in zip(
                chain.vintage_chain.releases,
                chain.schedule_evidence,
                strict=True,
            )
            if evidence.known_at_ns <= decision
        ]
        if not visible_pairs:
            continue
        release, evidence = visible_pairs[-1]
        if not start <= release.scheduled_for_ns < end:
            continue
        if families and release.event_family.value not in families:
            continue
        if (
            not include_cancelled
            and release.status is EconomicReleaseStatus.CANCELLED
        ):
            continue
        states.append(
            EconomicReleaseScheduleStateV1(
                chain_id=chain.chain_id,
                expected_occurrence_id=(
                    None
                    if chain.expected_occurrence is None
                    or chain.expected_occurrence.status_known_at_ns > decision
                    else chain.expected_occurrence.occurrence_id
                ),
                decision_at_ns=decision,
                visible_release_ids=tuple(
                    item.release_id for item, _ in visible_pairs
                ),
                release=release,
                evidence=evidence,
            )
        )
    if len(states) > maximum:
        raise ValueError("schedule query exceeds requested event bound")
    return EconomicReleaseScheduleQueryV1(
        corpus_id=corpus.corpus_id,
        start_ns=start,
        end_ns=end,
        decision_at_ns=decision,
        states=tuple(states),
        limitations=corpus.limitations,
    )


def audit_economic_release_schedules(
    corpus: EconomicReleaseScheduleCorpusV1,
) -> EconomicReleaseScheduleAuditV1:
    """Verify schedule transitions at every before/at knowledge boundary."""
    if not isinstance(corpus, EconomicReleaseScheduleCorpusV1):
        raise TypeError("schedule audit requires a v1 schedule corpus")
    counts: Counter[str] = Counter()
    rescheduled: list[str] = []
    cancelled: list[str] = []
    unscheduled: list[str] = []
    mismatches: list[str] = []
    no_future: list[str] = []
    for chain in corpus.chains:
        releases = chain.vintage_chain.releases
        event_key = releases[0].logical_event_key
        for index, (release, evidence) in enumerate(
            zip(releases, chain.schedule_evidence, strict=True)
        ):
            counts[evidence.exception_kind.value] += 1
            before = [
                item
                for item in releases
                if item.available_at_ns < evidence.known_at_ns
            ]
            at = [
                item
                for item in releases
                if item.available_at_ns <= evidence.known_at_ns
            ]
            expected_before = index
            expected_at = index + 1
            if (
                len(before) != expected_before
                or len(at) != expected_at
                or at[-1].release_id != release.release_id
            ):
                no_future.append(f"{event_key}:{index}")
        statuses = {item.status for item in releases}
        if (
            EconomicReleaseStatus.RESCHEDULED in statuses
            or EconomicReleaseStatus.DELAYED in statuses
        ):
            rescheduled.append(event_key)
        final = releases[-1]
        if final.status is EconomicReleaseStatus.CANCELLED:
            cancelled.append(event_key)
        if chain.expected_occurrence is None:
            unscheduled.append(event_key)
        else:
            expected_status = _expected_status_for_release(final.status)
            if chain.expected_occurrence.status is not expected_status:
                mismatches.append(event_key)
    return EconomicReleaseScheduleAuditV1(
        corpus_id=corpus.corpus_id,
        catalog_id=corpus.catalog_id,
        chain_count=len(corpus.chains),
        schedule_vintage_count=sum(
            len(item.schedule_evidence) for item in corpus.chains
        ),
        exception_counts=tuple(counts.items()),
        rescheduled_event_keys=tuple(rescheduled),
        cancelled_event_keys=tuple(cancelled),
        unscheduled_event_keys=tuple(unscheduled),
        occurrence_status_mismatches=tuple(mismatches),
        no_future_violations=tuple(no_future),
    )


def write_economic_release_schedule_corpus(
    corpus: EconomicReleaseScheduleCorpusV1, path: str | Path
) -> Path:
    destination = Path(path)
    payload = corpus.to_json().encode("utf-8")
    if len(payload) > MAX_ECONOMIC_RELEASE_SCHEDULE_CORPUS_BYTES:
        raise ValueError("release schedule corpus exceeds byte bound")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload + b"\n")
    return destination


def read_economic_release_schedule_corpus(
    path: str | Path,
) -> EconomicReleaseScheduleCorpusV1:
    source = Path(path)
    if source.stat().st_size > MAX_ECONOMIC_RELEASE_SCHEDULE_CORPUS_BYTES:
        raise ValueError("release schedule corpus exceeds byte bound")
    try:
        return EconomicReleaseScheduleCorpusV1.from_json(
            source.read_text(encoding="utf-8")
        )
    except UnicodeDecodeError as exc:
        raise ValueError("release schedule corpus is not UTF-8") from exc


def _validate_schedule_exception(
    release: EconomicCalendarReleaseV1,
    evidence: EconomicReleaseScheduleEvidenceV1,
    unscheduled: bool,
) -> None:
    kind = evidence.exception_kind
    if unscheduled:
        if release.revision_sequence == 0:
            if (
                kind is not EconomicScheduleExceptionKind.UNSCHEDULED_EMERGENCY
                or release.status is not EconomicReleaseStatus.UNSCHEDULED
            ):
                raise ValueError(
                    "unscheduled event requires emergency schedule evidence"
                )
        elif kind is not EconomicScheduleExceptionKind.ROUTINE:
            raise ValueError(
                "later unscheduled-event revisions cannot invent schedule notice"
            )
        return
    required: Mapping[
        EconomicReleaseStatus, frozenset[EconomicScheduleExceptionKind]
    ] = {
        EconomicReleaseStatus.RESCHEDULED: frozenset(
            {
                EconomicScheduleExceptionKind.RESCHEDULE,
                EconomicScheduleExceptionKind.HOLIDAY_SHIFT,
                EconomicScheduleExceptionKind.DST_TRANSITION,
            }
        ),
        EconomicReleaseStatus.DELAYED: frozenset(
            {EconomicScheduleExceptionKind.PRODUCER_DELAY}
        ),
        EconomicReleaseStatus.CANCELLED: frozenset(
            {EconomicScheduleExceptionKind.CANCELLATION}
        ),
        EconomicReleaseStatus.SOURCE_CALENDAR_UNAVAILABLE: frozenset(
            {EconomicScheduleExceptionKind.SOURCE_CALENDAR_OUTAGE}
        ),
    }
    allowed = required.get(release.status)
    if allowed is not None and kind not in allowed:
        raise ValueError("schedule status lacks matching exception evidence")
    if allowed is None and kind in {
        EconomicScheduleExceptionKind.RESCHEDULE,
        EconomicScheduleExceptionKind.PRODUCER_DELAY,
        EconomicScheduleExceptionKind.CANCELLATION,
        EconomicScheduleExceptionKind.UNSCHEDULED_EMERGENCY,
        EconomicScheduleExceptionKind.SOURCE_CALENDAR_OUTAGE,
    }:
        raise ValueError("schedule exception differs from release status")
    if (
        kind is EconomicScheduleExceptionKind.DST_TRANSITION
        and evidence.dst_fold is None
    ):
        raise ValueError("DST transition evidence requires an explicit fold")


def _expected_status_for_release(
    status: EconomicReleaseStatus,
) -> EconomicExpectedOccurrenceStatus:
    mapping = {
        EconomicReleaseStatus.TENTATIVE: EconomicExpectedOccurrenceStatus.SCHEDULED,
        EconomicReleaseStatus.SCHEDULED: EconomicExpectedOccurrenceStatus.SCHEDULED,
        EconomicReleaseStatus.RESCHEDULED: EconomicExpectedOccurrenceStatus.RESCHEDULED,
        EconomicReleaseStatus.DELAYED: EconomicExpectedOccurrenceStatus.RESCHEDULED,
        EconomicReleaseStatus.CANCELLED: EconomicExpectedOccurrenceStatus.CANCELLED,
        EconomicReleaseStatus.RELEASED: EconomicExpectedOccurrenceStatus.RELEASED,
        EconomicReleaseStatus.DISCONTINUED: EconomicExpectedOccurrenceStatus.DISCONTINUED,
        EconomicReleaseStatus.SOURCE_CALENDAR_UNAVAILABLE: EconomicExpectedOccurrenceStatus.SOURCE_UNAVAILABLE,
        EconomicReleaseStatus.UNRESOLVED: EconomicExpectedOccurrenceStatus.UNRESOLVED,
    }
    if status is EconomicReleaseStatus.UNSCHEDULED:
        raise ValueError("unscheduled release has no expected occurrence")
    return mapping[status]


__all__ = [
    "ECONOMIC_RELEASE_SCHEDULE_AUDIT_SCHEMA_VERSION",
    "ECONOMIC_RELEASE_SCHEDULE_CHAIN_SCHEMA_VERSION",
    "ECONOMIC_RELEASE_SCHEDULE_CORPUS_SCHEMA_VERSION",
    "ECONOMIC_RELEASE_SCHEDULE_EVIDENCE_SCHEMA_VERSION",
    "ECONOMIC_RELEASE_SCHEDULE_QUERY_SCHEMA_VERSION",
    "ECONOMIC_RELEASE_SCHEDULE_STATE_SCHEMA_VERSION",
    "MAX_ECONOMIC_RELEASE_SCHEDULE_CHAINS",
    "MAX_ECONOMIC_RELEASE_SCHEDULE_CORPUS_BYTES",
    "MAX_ECONOMIC_RELEASE_SCHEDULE_QUERY_EVENTS",
    "EconomicReleaseScheduleAuditV1",
    "EconomicReleaseScheduleChainV1",
    "EconomicReleaseScheduleCorpusV1",
    "EconomicReleaseScheduleEvidenceV1",
    "EconomicReleaseScheduleQueryV1",
    "EconomicReleaseScheduleStateV1",
    "EconomicScheduleExceptionKind",
    "audit_economic_release_schedules",
    "build_economic_release_schedule_corpus",
    "query_economic_release_schedule_as_known",
    "read_economic_release_schedule_corpus",
    "write_economic_release_schedule_corpus",
]
