"""Deterministic economic-release time and vintage reconstruction.

This module turns already-retained official archive observations into the
provider-neutral release contracts defined by :mod:`economic_calendar`.  It
does not fetch archives or infer unavailable source history.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from histdatacom.market_context.contracts import (
    MarketContextPrecision,
    MarketContextSourceV1,
    canonical_contract_json,
)
from histdatacom.market_context.economic_calendar import (
    MAX_ECONOMIC_CALENDAR_QUERY_EVENTS,
    EconomicCalendarCorpusV1,
    EconomicCalendarForecastV1,
    EconomicCalendarReleaseV1,
    EconomicReleaseStage,
    EconomicReleaseStatus,
    EconomicTimePrecision,
    EconomicUnitConversionV1,
    query_economic_calendar_as_known,
)
from histdatacom.runtime_contracts import JSONValue

ECONOMIC_RELEASE_TIME_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.economic-release-time-evidence.v1"
)
ECONOMIC_RELEASE_VINTAGE_MUTATION_SCHEMA_VERSION = (
    "histdatacom.economic-release-vintage-mutation.v1"
)
ECONOMIC_RELEASE_VINTAGE_CHAIN_SCHEMA_VERSION = (
    "histdatacom.economic-release-vintage-chain.v1"
)
ECONOMIC_RELEASE_VINTAGE_AUDIT_SCHEMA_VERSION = (
    "histdatacom.economic-release-vintage-audit.v1"
)

MAX_ECONOMIC_RELEASE_VINTAGES_PER_EVENT = 10_001


class EconomicReleaseAvailabilityBasis(str, Enum):
    """Evidence used for the earliest defensible strategy admission time."""

    OFFICIAL_PUBLICATION = "official-publication"
    SOURCE_UPDATE = "source-update-upper-bound"
    FIRST_OBSERVED = "first-observed"
    INFERRED_BOUNDED = "inferred-bounded"

    @classmethod
    def from_value(
        cls, value: str | EconomicReleaseAvailabilityBasis
    ) -> EconomicReleaseAvailabilityBasis:
        """Return a strict availability basis."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported release availability basis") from exc


class EconomicReleaseRevisionKind(str, Enum):
    """Meaning of a later value vintage within one semantic series era."""

    ROUTINE = "routine"
    CORRECTION = "correction"
    SEASONAL_ADJUSTMENT = "seasonal-adjustment"
    SIMULTANEOUS_PREVIOUS = "simultaneous-previous"
    BENCHMARK = "benchmark"

    @classmethod
    def from_value(
        cls, value: str | EconomicReleaseRevisionKind
    ) -> EconomicReleaseRevisionKind:
        """Return a strict revision kind."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported release revision kind") from exc


def _required_text(value: object, name: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    return str(value).strip() or None


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


def _mapping(value: object, name: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return value


def _sequence(value: object, name: str = "sequence") -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _text_tuple(value: Iterable[object], name: str) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item, name) for item in value}))
    if len(result) > 64:
        raise ValueError(f"{name} exceeds the item bound")
    return result


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
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field}_lexical is not ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field}_lexical must retain an explicit offset")
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


@dataclass(frozen=True, slots=True)
class EconomicReleaseTimeEvidenceV1:
    """Separate schedule, publication, observation, and source-update time."""

    scheduled_for_ns: int
    scheduled_lexical: str
    actual_published_at_ns: int | None
    actual_published_lexical: str | None
    first_observed_at_ns: int
    source_updated_at_ns: int | None
    source_updated_lexical: str | None
    available_at_ns: int
    publication_not_before_ns: int | None
    publication_not_after_ns: int | None
    source_timezone: str
    timezone_evidence: str
    time_precision: EconomicTimePrecision
    precision: MarketContextPrecision
    availability_basis: EconomicReleaseAvailabilityBasis
    evidence_id: str = ""
    schema_version: str = ECONOMIC_RELEASE_TIME_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_RELEASE_TIME_EVIDENCE_SCHEMA_VERSION:
            raise ValueError(
                "unsupported economic release time evidence schema"
            )
        scheduled = _bounded_ns(self.scheduled_for_ns, "scheduled_for_ns")
        actual = _optional_ns(
            self.actual_published_at_ns, "actual_published_at_ns"
        )
        observed = _bounded_ns(
            self.first_observed_at_ns, "first_observed_at_ns"
        )
        updated = _optional_ns(
            self.source_updated_at_ns, "source_updated_at_ns"
        )
        available = _bounded_ns(self.available_at_ns, "available_at_ns")
        lower = _optional_ns(
            self.publication_not_before_ns, "publication_not_before_ns"
        )
        upper = _optional_ns(
            self.publication_not_after_ns, "publication_not_after_ns"
        )
        scheduled_lexical, zone_name = _time_evidence(
            timestamp_ns=scheduled,
            lexical=self.scheduled_lexical,
            timezone_name=self.source_timezone,
            field="scheduled",
        )
        actual_lexical = _optional_text(self.actual_published_lexical)
        if actual is None:
            if actual_lexical is not None:
                raise ValueError("publication lexical evidence requires a time")
        else:
            actual_lexical, actual_zone = _time_evidence(
                timestamp_ns=actual,
                lexical=actual_lexical,
                timezone_name=zone_name,
                field="actual_published",
            )
            if actual_zone != zone_name:
                raise ValueError("publication and schedule timezones differ")
            if actual > observed:
                raise ValueError("publication follows first observation")
        updated_lexical = _optional_text(self.source_updated_lexical)
        if updated is None:
            if updated_lexical is not None:
                raise ValueError(
                    "source-update lexical evidence requires a time"
                )
        else:
            updated_lexical, updated_zone = _time_evidence(
                timestamp_ns=updated,
                lexical=updated_lexical,
                timezone_name=zone_name,
                field="source_updated",
            )
            if updated_zone != zone_name:
                raise ValueError("source-update and schedule timezones differ")
            if updated > observed:
                raise ValueError("source update follows first observation")
            if actual is not None and updated < actual:
                raise ValueError("source update precedes official publication")
        if available > observed:
            raise ValueError("availability follows first observation")
        if (lower is None) != (upper is None):
            raise ValueError("publication bounds must be supplied together")
        if lower is not None and upper is not None:
            if not lower <= upper <= observed:
                raise ValueError("publication bounds are inconsistent")
            if actual is not None and not lower <= actual <= upper:
                raise ValueError(
                    "publication time lies outside retained bounds"
                )
        basis = EconomicReleaseAvailabilityBasis.from_value(
            self.availability_basis
        )
        precision = EconomicTimePrecision.from_value(self.time_precision)
        if basis is EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION:
            if actual is None or available != actual:
                raise ValueError(
                    "official-publication availability requires its exact time"
                )
            if precision not in {
                EconomicTimePrecision.EXACT_SECOND,
                EconomicTimePrecision.EXACT_MINUTE,
            }:
                raise ValueError(
                    "official-publication basis requires exact precision"
                )
        elif basis is EconomicReleaseAvailabilityBasis.SOURCE_UPDATE:
            if updated is None or available != updated:
                raise ValueError(
                    "source-update availability requires its update time"
                )
        elif basis is EconomicReleaseAvailabilityBasis.FIRST_OBSERVED:
            if available != observed:
                raise ValueError(
                    "first-observed availability must equal observation time"
                )
        else:
            if lower is None or upper is None or available != upper:
                raise ValueError(
                    "inferred availability requires a bounded upper endpoint"
                )
            if precision not in {
                EconomicTimePrecision.DATE_ONLY,
                EconomicTimePrecision.INFERRED_BOUNDED,
            }:
                raise ValueError(
                    "inferred availability requires bounded time precision"
                )
        if actual is not None and precision not in {
            EconomicTimePrecision.EXACT_SECOND,
            EconomicTimePrecision.EXACT_MINUTE,
        }:
            raise ValueError(
                "an actual publication time requires exact time precision"
            )
        object.__setattr__(self, "scheduled_for_ns", scheduled)
        object.__setattr__(self, "scheduled_lexical", scheduled_lexical)
        object.__setattr__(self, "actual_published_at_ns", actual)
        object.__setattr__(self, "actual_published_lexical", actual_lexical)
        object.__setattr__(self, "first_observed_at_ns", observed)
        object.__setattr__(self, "source_updated_at_ns", updated)
        object.__setattr__(self, "source_updated_lexical", updated_lexical)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "publication_not_before_ns", lower)
        object.__setattr__(self, "publication_not_after_ns", upper)
        object.__setattr__(self, "source_timezone", zone_name)
        object.__setattr__(
            self,
            "timezone_evidence",
            _required_text(self.timezone_evidence, "timezone_evidence"),
        )
        object.__setattr__(self, "time_precision", precision)
        object.__setattr__(
            self,
            "precision",
            MarketContextPrecision.from_value(self.precision),
        )
        object.__setattr__(self, "availability_basis", basis)
        expected = _stable_id(
            "economic-release-time-evidence", self.identity_payload()
        )
        supplied = _optional_text(self.evidence_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "evidence_id does not match deterministic identity"
            )
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return complete deterministic time evidence."""
        return {
            "schema_version": self.schema_version,
            "scheduled_for_ns": self.scheduled_for_ns,
            "scheduled_lexical": self.scheduled_lexical,
            "actual_published_at_ns": self.actual_published_at_ns,
            "actual_published_lexical": self.actual_published_lexical,
            "first_observed_at_ns": self.first_observed_at_ns,
            "source_updated_at_ns": self.source_updated_at_ns,
            "source_updated_lexical": self.source_updated_lexical,
            "available_at_ns": self.available_at_ns,
            "publication_not_before_ns": self.publication_not_before_ns,
            "publication_not_after_ns": self.publication_not_after_ns,
            "source_timezone": self.source_timezone,
            "timezone_evidence": self.timezone_evidence,
            "time_precision": self.time_precision.value,
            "precision": self.precision.value,
            "availability_basis": self.availability_basis.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible time evidence."""
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicReleaseTimeEvidenceV1:
        """Restore and verify time evidence."""
        return cls(
            scheduled_for_ns=cast(int, data.get("scheduled_for_ns")),
            scheduled_lexical=str(data.get("scheduled_lexical", "")),
            actual_published_at_ns=cast(
                int | None, data.get("actual_published_at_ns")
            ),
            actual_published_lexical=_optional_text(
                data.get("actual_published_lexical")
            ),
            first_observed_at_ns=cast(int, data.get("first_observed_at_ns")),
            source_updated_at_ns=cast(
                int | None, data.get("source_updated_at_ns")
            ),
            source_updated_lexical=_optional_text(
                data.get("source_updated_lexical")
            ),
            available_at_ns=cast(int, data.get("available_at_ns")),
            publication_not_before_ns=cast(
                int | None, data.get("publication_not_before_ns")
            ),
            publication_not_after_ns=cast(
                int | None, data.get("publication_not_after_ns")
            ),
            source_timezone=str(data.get("source_timezone", "")),
            timezone_evidence=str(data.get("timezone_evidence", "")),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            precision=MarketContextPrecision.from_value(
                str(data.get("precision", ""))
            ),
            availability_basis=EconomicReleaseAvailabilityBasis.from_value(
                str(data.get("availability_basis", ""))
            ),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicReleaseTimeEvidenceV1:
        """Restore time evidence from deterministic JSON."""
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("release time evidence is invalid JSON") from exc
        return cls.from_dict(_mapping(data))


def select_release_time_evidence(
    candidates: Sequence[EconomicReleaseTimeEvidenceV1],
) -> EconomicReleaseTimeEvidenceV1:
    """Select deterministically from duplicate mirrors by evidence strength."""
    values = tuple(candidates)
    if not values:
        raise ValueError("at least one release-time candidate is required")
    if any(
        not isinstance(item, EconomicReleaseTimeEvidenceV1) for item in values
    ):
        raise TypeError("release-time candidates must use the v1 contract")
    scheduled = values[0].scheduled_for_ns
    if any(item.scheduled_for_ns != scheduled for item in values[1:]):
        raise ValueError("release-time candidates disagree on scheduled time")
    publication_times = {
        item.actual_published_at_ns
        for item in values
        if item.actual_published_at_ns is not None
    }
    if len(publication_times) > 1:
        raise ValueError(
            "release-time candidates disagree on official publication time"
        )
    basis_rank = {
        EconomicReleaseAvailabilityBasis.OFFICIAL_PUBLICATION: 0,
        EconomicReleaseAvailabilityBasis.SOURCE_UPDATE: 1,
        EconomicReleaseAvailabilityBasis.FIRST_OBSERVED: 2,
        EconomicReleaseAvailabilityBasis.INFERRED_BOUNDED: 3,
    }
    precision_rank = {
        EconomicTimePrecision.EXACT_SECOND: 0,
        EconomicTimePrecision.EXACT_MINUTE: 1,
        EconomicTimePrecision.SCHEDULED_ONLY: 2,
        EconomicTimePrecision.DATE_ONLY: 3,
        EconomicTimePrecision.INFERRED_BOUNDED: 4,
    }
    return min(
        values,
        key=lambda item: (
            basis_rank[item.availability_basis],
            precision_rank[item.time_precision],
            item.available_at_ns,
            item.first_observed_at_ns,
            item.evidence_id,
        ),
    )


def _time_evidence_matches_release(
    evidence: EconomicReleaseTimeEvidenceV1,
    release: EconomicCalendarReleaseV1,
) -> bool:
    return (
        evidence.scheduled_for_ns == release.scheduled_for_ns
        and evidence.scheduled_lexical == release.scheduled_lexical
        and evidence.actual_published_at_ns == release.released_at_ns
        and evidence.actual_published_lexical == release.released_lexical
        and evidence.first_observed_at_ns == release.first_observed_at_ns
        and evidence.available_at_ns == release.available_at_ns
        and evidence.source_timezone == release.source_timezone
        and evidence.timezone_evidence == release.timezone_evidence
        and evidence.time_precision is release.time_precision
        and evidence.precision is release.precision
    )


@dataclass(frozen=True, slots=True)
class EconomicReleaseVintageMutationV1:
    """One observed status, schedule, publication, or value transition."""

    previous_release_id: str
    logical_event_key: str
    series_id: str
    source_release_id: str
    source_request_id: str
    stage: EconomicReleaseStage
    status: EconomicReleaseStatus
    time_evidence: EconomicReleaseTimeEvidenceV1
    source: MarketContextSourceV1
    limitations: tuple[str, ...]
    schedule_change_reason: str | None = None
    actual_value: float | None = None
    actual_lexical: str | None = None
    value_conversion: EconomicUnitConversionV1 | None = None
    content_sha256: str | None = None
    revision_kind: EconomicReleaseRevisionKind | None = None
    affected_reference_periods: tuple[str, ...] = ()
    triggering_logical_event_key: str | None = None
    mutation_id: str = ""
    schema_version: str = ECONOMIC_RELEASE_VINTAGE_MUTATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != ECONOMIC_RELEASE_VINTAGE_MUTATION_SCHEMA_VERSION
        ):
            raise ValueError("unsupported economic release mutation schema")
        for name in (
            "previous_release_id",
            "logical_event_key",
            "series_id",
            "source_release_id",
            "source_request_id",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        stage = EconomicReleaseStage.from_value(self.stage)
        status = EconomicReleaseStatus.from_value(self.status)
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "status", status)
        if not isinstance(self.time_evidence, EconomicReleaseTimeEvidenceV1):
            raise TypeError("time_evidence must use the v1 contract")
        if not isinstance(self.source, MarketContextSourceV1):
            raise TypeError("source must use MarketContextSourceV1")
        if self.source.retrieved_at_ns < max(
            self.time_evidence.first_observed_at_ns,
            self.time_evidence.available_at_ns,
        ):
            raise ValueError("source retrieval precedes observation")
        limitations = _text_tuple(self.limitations, "limitation")
        if not limitations:
            raise ValueError("a release mutation requires limitations")
        object.__setattr__(self, "limitations", limitations)
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
            raise ValueError("schedule/status changes require a reason")
        object.__setattr__(self, "schedule_change_reason", schedule_reason)
        actual_status = status in {
            EconomicReleaseStatus.RELEASED,
            EconomicReleaseStatus.UNSCHEDULED,
        }
        actual_lexical = _optional_text(self.actual_lexical)
        if actual_status:
            if self.actual_value is None or actual_lexical is None:
                raise ValueError(
                    "an actual publication requires value evidence"
                )
            if self.time_evidence.actual_published_at_ns is None:
                raise ValueError("an actual publication requires its time")
        elif (
            self.actual_value is not None
            or actual_lexical is not None
            or self.time_evidence.actual_published_at_ns is not None
        ):
            raise ValueError(
                "a non-publication mutation cannot contain an actual"
            )
        object.__setattr__(self, "actual_lexical", actual_lexical)
        if self.value_conversion is not None and not isinstance(
            self.value_conversion, EconomicUnitConversionV1
        ):
            raise TypeError("value_conversion must use the v1 contract")
        content_hash = _optional_text(self.content_sha256)
        if (
            content_hash is not None
            and content_hash != self.source.content_sha256
        ):
            raise ValueError("mutation content hash differs from source")
        object.__setattr__(self, "content_sha256", content_hash)
        revision_kind = self.revision_kind
        if revision_kind is not None:
            revision_kind = EconomicReleaseRevisionKind.from_value(
                revision_kind
            )
        if stage is EconomicReleaseStage.REVISION:
            if not actual_status or revision_kind is None:
                raise ValueError("a value revision requires a revision kind")
        elif revision_kind is not None:
            raise ValueError("revision kind is valid only for revision stage")
        object.__setattr__(self, "revision_kind", revision_kind)
        periods = _text_tuple(
            self.affected_reference_periods, "affected_reference_period"
        )
        if revision_kind is not None and not periods:
            raise ValueError("a value revision requires affected periods")
        trigger = _optional_text(self.triggering_logical_event_key)
        if (
            revision_kind is EconomicReleaseRevisionKind.BENCHMARK
            and len(periods) < 2
        ):
            raise ValueError(
                "a benchmark revision must identify multiple periods"
            )
        if revision_kind is EconomicReleaseRevisionKind.SIMULTANEOUS_PREVIOUS:
            if trigger is None or trigger == self.logical_event_key:
                raise ValueError(
                    "a simultaneous previous revision requires another event"
                )
            if not periods:
                raise ValueError(
                    "a simultaneous previous revision requires affected periods"
                )
        elif trigger is not None:
            raise ValueError(
                "triggering event is reserved for simultaneous previous revisions"
            )
        object.__setattr__(self, "affected_reference_periods", periods)
        object.__setattr__(self, "triggering_logical_event_key", trigger)
        expected = _stable_id(
            "economic-release-vintage-mutation", self.identity_payload()
        )
        supplied = _optional_text(self.mutation_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "mutation_id does not match deterministic identity"
            )
        object.__setattr__(self, "mutation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic transition payload."""
        return {
            "schema_version": self.schema_version,
            "previous_release_id": self.previous_release_id,
            "logical_event_key": self.logical_event_key,
            "series_id": self.series_id,
            "source_release_id": self.source_release_id,
            "source_request_id": self.source_request_id,
            "stage": self.stage.value,
            "status": self.status.value,
            "time_evidence": self.time_evidence.to_dict(),
            "source": self.source.to_dict(),
            "limitations": list(self.limitations),
            "schedule_change_reason": self.schedule_change_reason,
            "actual_value": self.actual_value,
            "actual_lexical": self.actual_lexical,
            "value_conversion": (
                None
                if self.value_conversion is None
                else self.value_conversion.to_dict()
            ),
            "content_sha256": self.content_sha256,
            "revision_kind": (
                None if self.revision_kind is None else self.revision_kind.value
            ),
            "affected_reference_periods": list(self.affected_reference_periods),
            "triggering_logical_event_key": self.triggering_logical_event_key,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible mutation evidence."""
        return {**self.identity_payload(), "mutation_id": self.mutation_id}

    def to_json(self) -> str:
        """Return deterministic compact mutation JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicReleaseVintageMutationV1:
        """Restore and verify one mutation."""
        return cls(
            previous_release_id=str(data.get("previous_release_id", "")),
            logical_event_key=str(data.get("logical_event_key", "")),
            series_id=str(data.get("series_id", "")),
            source_release_id=str(data.get("source_release_id", "")),
            source_request_id=str(data.get("source_request_id", "")),
            stage=EconomicReleaseStage.from_value(str(data.get("stage", ""))),
            status=EconomicReleaseStatus.from_value(
                str(data.get("status", ""))
            ),
            time_evidence=EconomicReleaseTimeEvidenceV1.from_dict(
                _mapping(data.get("time_evidence"))
            ),
            source=MarketContextSourceV1.from_dict(
                _mapping(data.get("source"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            schedule_change_reason=_optional_text(
                data.get("schedule_change_reason")
            ),
            actual_value=cast(float | None, data.get("actual_value")),
            actual_lexical=_optional_text(data.get("actual_lexical")),
            value_conversion=(
                None
                if data.get("value_conversion") is None
                else EconomicUnitConversionV1.from_dict(
                    _mapping(data.get("value_conversion"))
                )
            ),
            content_sha256=_optional_text(data.get("content_sha256")),
            revision_kind=(
                None
                if data.get("revision_kind") is None
                else EconomicReleaseRevisionKind.from_value(
                    str(data.get("revision_kind"))
                )
            ),
            affected_reference_periods=tuple(
                str(item)
                for item in _sequence(data.get("affected_reference_periods"))
            ),
            triggering_logical_event_key=_optional_text(
                data.get("triggering_logical_event_key")
            ),
            mutation_id=str(data.get("mutation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicReleaseVintageMutationV1:
        """Restore one mutation from deterministic JSON."""
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "release vintage mutation is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(data))


def apply_economic_release_vintage_mutation(
    previous: EconomicCalendarReleaseV1,
    mutation: EconomicReleaseVintageMutationV1,
) -> EconomicCalendarReleaseV1:
    """Apply one immutable transition without permitting semantic drift."""
    if not isinstance(previous, EconomicCalendarReleaseV1):
        raise TypeError("previous must use EconomicCalendarReleaseV1")
    if not isinstance(mutation, EconomicReleaseVintageMutationV1):
        raise TypeError("mutation must use EconomicReleaseVintageMutationV1")
    if mutation.previous_release_id != previous.release_id:
        raise ValueError("mutation does not name its exact predecessor")
    if mutation.logical_event_key != previous.logical_event_key:
        raise ValueError("mutation belongs to another logical event")
    if mutation.series_id != previous.series_id:
        raise ValueError(
            "series migrations, rebases, or semantic breaks require a new chain"
        )
    if (
        mutation.stage is not EconomicReleaseStage.REVISION
        and mutation.stage is not previous.stage
    ):
        raise ValueError("a distinct publication stage requires a new chain")
    if (
        previous.stage is EconomicReleaseStage.REVISION
        and mutation.stage is not EconomicReleaseStage.REVISION
    ):
        raise ValueError("a revision chain cannot return to publication stage")
    if (
        mutation.stage is EconomicReleaseStage.REVISION
        and previous.actual_value is None
    ):
        raise ValueError("a revision requires an earlier published actual")
    if (
        previous.actual_value is not None
        and mutation.stage is not EconomicReleaseStage.REVISION
    ):
        raise ValueError("a published actual can change only through revision")
    if mutation.time_evidence.available_at_ns <= previous.available_at_ns:
        raise ValueError("mutation availability must move strictly forward")
    evidence = mutation.time_evidence
    result = replace(
        previous,
        source_release_id=mutation.source_release_id,
        source_request_id=mutation.source_request_id,
        stage=mutation.stage,
        status=mutation.status,
        scheduled_for_ns=evidence.scheduled_for_ns,
        scheduled_lexical=evidence.scheduled_lexical,
        released_at_ns=evidence.actual_published_at_ns,
        released_lexical=evidence.actual_published_lexical,
        first_observed_at_ns=evidence.first_observed_at_ns,
        available_at_ns=evidence.available_at_ns,
        source_timezone=evidence.source_timezone,
        timezone_evidence=evidence.timezone_evidence,
        time_precision=evidence.time_precision,
        precision=evidence.precision,
        source=mutation.source,
        limitations=mutation.limitations,
        schedule_change_reason=mutation.schedule_change_reason,
        value_conversion=mutation.value_conversion,
        actual_value=mutation.actual_value,
        actual_lexical=mutation.actual_lexical,
        content_sha256=mutation.content_sha256,
        revision_sequence=previous.revision_sequence + 1,
        supersedes_release_id=previous.release_id,
        release_id="",
    )
    if result.semantic_key() != previous.semantic_key():
        raise AssertionError("release mutation changed semantic identity")
    return result


@dataclass(frozen=True, slots=True)
class EconomicReleaseVintageChainV1:
    """Replayable archive-to-release reconstruction for one logical event."""

    initial_release: EconomicCalendarReleaseV1
    initial_time_evidence: EconomicReleaseTimeEvidenceV1
    mutations: tuple[EconomicReleaseVintageMutationV1, ...]
    releases: tuple[EconomicCalendarReleaseV1, ...]
    limitations: tuple[str, ...]
    chain_id: str = ""
    schema_version: str = ECONOMIC_RELEASE_VINTAGE_CHAIN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_RELEASE_VINTAGE_CHAIN_SCHEMA_VERSION:
            raise ValueError(
                "unsupported economic release vintage chain schema"
            )
        if not isinstance(self.initial_release, EconomicCalendarReleaseV1):
            raise TypeError("initial_release must use the v1 contract")
        if self.initial_release.revision_sequence != 0:
            raise ValueError(
                "a vintage chain must start at revision sequence zero"
            )
        if not isinstance(
            self.initial_time_evidence, EconomicReleaseTimeEvidenceV1
        ):
            raise TypeError("initial_time_evidence must use the v1 contract")
        if not _time_evidence_matches_release(
            self.initial_time_evidence, self.initial_release
        ):
            raise ValueError(
                "initial time evidence differs from initial release"
            )
        if len(self.mutations) + 1 > MAX_ECONOMIC_RELEASE_VINTAGES_PER_EVENT:
            raise ValueError("release vintage chain exceeds its bound")
        if any(
            not isinstance(item, EconomicReleaseVintageMutationV1)
            for item in self.mutations
        ):
            raise TypeError("chain mutations must use the v1 contract")
        if any(
            not isinstance(item, EconomicCalendarReleaseV1)
            for item in self.releases
        ):
            raise TypeError("chain releases must use the calendar v1 contract")
        replayed = self._replay()
        if replayed != tuple(self.releases):
            raise ValueError("retained releases differ from mutation replay")
        limitations = _text_tuple(self.limitations, "limitation")
        if not limitations:
            raise ValueError("a release vintage chain requires limitations")
        object.__setattr__(self, "releases", replayed)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "economic-release-vintage-chain", self.identity_payload()
        )
        supplied = _optional_text(self.chain_id)
        if supplied is not None and supplied != expected:
            raise ValueError("chain_id does not match deterministic identity")
        object.__setattr__(self, "chain_id", expected)

    def _replay(self) -> tuple[EconomicCalendarReleaseV1, ...]:
        values = [self.initial_release]
        for mutation in self.mutations:
            values.append(
                apply_economic_release_vintage_mutation(values[-1], mutation)
            )
        return tuple(values)

    def replay(self) -> tuple[EconomicCalendarReleaseV1, ...]:
        """Reconstruct and return the exact immutable release chain."""
        replayed = self._replay()
        if replayed != self.releases:
            raise ValueError("release vintage replay changed")
        return replayed

    def as_known_at(
        self, decision_at_ns: int
    ) -> EconomicCalendarReleaseV1 | None:
        """Return the latest chain member visible at one decision time."""
        decision = _bounded_ns(decision_at_ns, "decision_at_ns")
        visible = [
            item for item in self.releases if item.available_at_ns <= decision
        ]
        return visible[-1] if visible else None

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic chain payload."""
        return {
            "schema_version": self.schema_version,
            "initial_release": self.initial_release.to_dict(),
            "initial_time_evidence": self.initial_time_evidence.to_dict(),
            "mutations": [item.to_dict() for item in self.mutations],
            "releases": [item.to_dict() for item in self.releases],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible reconstruction evidence."""
        return {**self.identity_payload(), "chain_id": self.chain_id}

    def to_json(self) -> str:
        """Return deterministic compact chain JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicReleaseVintageChainV1:
        """Restore and verify one release chain."""
        return cls(
            initial_release=EconomicCalendarReleaseV1.from_dict(
                _mapping(data.get("initial_release"))
            ),
            initial_time_evidence=EconomicReleaseTimeEvidenceV1.from_dict(
                _mapping(data.get("initial_time_evidence"))
            ),
            mutations=tuple(
                EconomicReleaseVintageMutationV1.from_dict(_mapping(item))
                for item in _sequence(data.get("mutations"))
            ),
            releases=tuple(
                EconomicCalendarReleaseV1.from_dict(_mapping(item))
                for item in _sequence(data.get("releases"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            chain_id=str(data.get("chain_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicReleaseVintageChainV1:
        """Restore a release chain from deterministic JSON."""
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("release vintage chain is invalid JSON") from exc
        return cls.from_dict(_mapping(data))


def build_economic_release_vintage_chain(
    initial_release: EconomicCalendarReleaseV1,
    *,
    initial_time_evidence: EconomicReleaseTimeEvidenceV1,
    mutations: Sequence[EconomicReleaseVintageMutationV1] = (),
    limitations: Sequence[str],
) -> EconomicReleaseVintageChainV1:
    """Reconstruct a complete immutable chain from ordered observations."""
    current = initial_release
    releases = [current]
    for mutation in tuple(mutations):
        current = apply_economic_release_vintage_mutation(current, mutation)
        releases.append(current)
    return EconomicReleaseVintageChainV1(
        initial_release=initial_release,
        initial_time_evidence=initial_time_evidence,
        mutations=tuple(mutations),
        releases=tuple(releases),
        limitations=tuple(limitations),
    )


def build_economic_calendar_corpus_from_vintage_chains(
    chains: Sequence[EconomicReleaseVintageChainV1],
    *,
    coverage_start_ns: int,
    coverage_end_ns: int,
    complete: bool,
    limitations: Sequence[str],
    forecasts: Sequence[EconomicCalendarForecastV1] = (),
) -> EconomicCalendarCorpusV1:
    """Assemble verified reconstructed chains into a queryable corpus."""
    values = tuple(chains)
    if any(
        not isinstance(item, EconomicReleaseVintageChainV1) for item in values
    ):
        raise TypeError("chains must use EconomicReleaseVintageChainV1")
    event_keys = [item.initial_release.logical_event_key for item in values]
    if len(set(event_keys)) != len(event_keys):
        raise ValueError("a logical event is represented by multiple chains")
    releases = tuple(release for chain in values for release in chain.replay())
    return EconomicCalendarCorpusV1(
        coverage_start_ns=coverage_start_ns,
        coverage_end_ns=coverage_end_ns,
        complete=complete,
        releases=releases,
        forecasts=tuple(forecasts),
        limitations=tuple(limitations),
    )


@dataclass(frozen=True, slots=True)
class EconomicReleaseVintageAuditV1:
    """Machine-verifiable coverage and point-in-time reconstruction audit."""

    corpus_id: str
    chain_ids: tuple[str, ...]
    event_count: int
    release_count: int
    initial_actual_count: int
    revision_count: int
    availability_basis_counts: tuple[tuple[str, int], ...]
    revision_kind_counts: tuple[tuple[str, int], ...]
    missing_initial_actual_event_keys: tuple[str, ...]
    checks: tuple[tuple[str, bool], ...]
    limitations: tuple[str, ...]
    audit_id: str = ""
    schema_version: str = ECONOMIC_RELEASE_VINTAGE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ECONOMIC_RELEASE_VINTAGE_AUDIT_SCHEMA_VERSION:
            raise ValueError("unsupported release vintage audit schema")
        object.__setattr__(
            self, "corpus_id", _required_text(self.corpus_id, "corpus_id")
        )
        chain_ids = tuple(
            sorted(_required_text(item, "chain_id") for item in self.chain_ids)
        )
        if len(set(chain_ids)) != len(chain_ids):
            raise ValueError("audit repeats a chain identity")
        object.__setattr__(self, "chain_ids", chain_ids)
        for name in (
            "event_count",
            "release_count",
            "initial_actual_count",
            "revision_count",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
            ):
                raise ValueError(f"{name} must be a non-negative integer")
        if self.event_count != len(chain_ids):
            raise ValueError("event_count differs from retained chains")
        if self.release_count < self.event_count:
            raise ValueError("release_count is smaller than event_count")
        if self.initial_actual_count > self.event_count:
            raise ValueError("initial_actual_count exceeds event_count")
        if self.revision_count > self.release_count - self.event_count:
            raise ValueError("revision_count exceeds later vintage count")
        basis_counts = tuple(sorted(self.availability_basis_counts))
        revision_counts = tuple(sorted(self.revision_kind_counts))
        if {name for name, _ in basis_counts} != {
            item.value for item in EconomicReleaseAvailabilityBasis
        }:
            raise ValueError("availability counts do not cover every basis")
        if {name for name, _ in revision_counts} != {
            item.value for item in EconomicReleaseRevisionKind
        }:
            raise ValueError("revision counts do not cover every kind")
        if any(
            isinstance(count, bool) or not isinstance(count, int) or count < 0
            for _, count in basis_counts + revision_counts
        ):
            raise ValueError("audit counts must be non-negative integers")
        if sum(count for _, count in basis_counts) != self.release_count:
            raise ValueError("availability counts differ from release_count")
        if sum(count for _, count in revision_counts) != self.revision_count:
            raise ValueError("revision-kind counts differ from revision_count")
        object.__setattr__(self, "availability_basis_counts", basis_counts)
        object.__setattr__(self, "revision_kind_counts", revision_counts)
        missing = _text_tuple(
            self.missing_initial_actual_event_keys, "logical_event_key"
        )
        object.__setattr__(self, "missing_initial_actual_event_keys", missing)
        if len(missing) + self.initial_actual_count != self.event_count:
            raise ValueError("initial-actual coverage differs from event_count")
        checks = tuple(sorted(self.checks))
        if (
            not checks
            or len({name for name, _ in checks}) != len(checks)
            or any(
                not _optional_text(name) or not isinstance(result, bool)
                for name, result in checks
            )
        ):
            raise ValueError("audit requires boolean integrity checks")
        if not all(result for _, result in checks):
            raise ValueError("release vintage audit contains a failed check")
        object.__setattr__(self, "checks", checks)
        limitations = _text_tuple(self.limitations, "limitation")
        if not limitations:
            raise ValueError("a release vintage audit requires limitations")
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "economic-release-vintage-audit", self.identity_payload()
        )
        supplied = _optional_text(self.audit_id)
        if supplied is not None and supplied != expected:
            raise ValueError("audit_id does not match deterministic identity")
        object.__setattr__(self, "audit_id", expected)

    @property
    def passed(self) -> bool:
        """Return whether all reconstruction-integrity checks passed."""
        return all(result for _, result in self.checks)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic audit payload."""
        return {
            "schema_version": self.schema_version,
            "corpus_id": self.corpus_id,
            "chain_ids": list(self.chain_ids),
            "event_count": self.event_count,
            "release_count": self.release_count,
            "initial_actual_count": self.initial_actual_count,
            "revision_count": self.revision_count,
            "availability_basis_counts": [
                {"basis": name, "count": count}
                for name, count in self.availability_basis_counts
            ],
            "revision_kind_counts": [
                {"kind": name, "count": count}
                for name, count in self.revision_kind_counts
            ],
            "missing_initial_actual_event_keys": list(
                self.missing_initial_actual_event_keys
            ),
            "checks": [
                {"name": name, "passed": passed} for name, passed in self.checks
            ],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible audit evidence."""
        return {**self.identity_payload(), "audit_id": self.audit_id}

    def to_json(self) -> str:
        """Return deterministic compact audit JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EconomicReleaseVintageAuditV1:
        """Restore and verify one reconstruction audit."""
        return cls(
            corpus_id=str(data.get("corpus_id", "")),
            chain_ids=tuple(
                str(item) for item in _sequence(data.get("chain_ids"))
            ),
            event_count=cast(int, data.get("event_count")),
            release_count=cast(int, data.get("release_count")),
            initial_actual_count=cast(int, data.get("initial_actual_count")),
            revision_count=cast(int, data.get("revision_count")),
            availability_basis_counts=tuple(
                (
                    str(_mapping(item, "availability count").get("basis", "")),
                    cast(
                        int,
                        _mapping(item, "availability count").get("count"),
                    ),
                )
                for item in _sequence(data.get("availability_basis_counts"))
            ),
            revision_kind_counts=tuple(
                (
                    str(_mapping(item, "revision count").get("kind", "")),
                    cast(int, _mapping(item, "revision count").get("count")),
                )
                for item in _sequence(data.get("revision_kind_counts"))
            ),
            missing_initial_actual_event_keys=tuple(
                str(item)
                for item in _sequence(
                    data.get("missing_initial_actual_event_keys")
                )
            ),
            checks=tuple(
                (
                    str(_mapping(item, "check").get("name", "")),
                    cast(bool, _mapping(item, "check").get("passed")),
                )
                for item in _sequence(data.get("checks"))
            ),
            limitations=tuple(
                str(item) for item in _sequence(data.get("limitations"))
            ),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> EconomicReleaseVintageAuditV1:
        """Restore an audit from deterministic JSON."""
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("release vintage audit is invalid JSON") from exc
        return cls.from_dict(_mapping(data))


def audit_economic_release_vintages(
    corpus: EconomicCalendarCorpusV1,
    chains: Sequence[EconomicReleaseVintageChainV1],
    *,
    limitations: Sequence[str],
) -> EconomicReleaseVintageAuditV1:
    """Audit replay, visibility, previous-as-known, and difficult cases."""
    if not isinstance(corpus, EconomicCalendarCorpusV1):
        raise TypeError("corpus must use EconomicCalendarCorpusV1")
    values = tuple(chains)
    if any(
        not isinstance(item, EconomicReleaseVintageChainV1) for item in values
    ):
        raise TypeError("chains must use EconomicReleaseVintageChainV1")
    reconstructed = tuple(item for chain in values for item in chain.replay())
    if tuple(corpus.releases) != tuple(
        sorted(
            reconstructed,
            key=lambda item: (
                item.logical_event_key,
                item.revision_sequence,
                item.release_id,
            ),
        )
    ):
        raise ValueError("corpus releases differ from reconstructed chains")
    basis_counts = {item.value: 0 for item in EconomicReleaseAvailabilityBasis}
    revision_counts = {item.value: 0 for item in EconomicReleaseRevisionKind}
    missing_initial: list[str] = []
    initial_actuals = 0
    revision_total = 0
    all_evidence: list[EconomicReleaseTimeEvidenceV1] = []
    first_actual_by_event: dict[str, EconomicCalendarReleaseV1] = {}
    for chain in values:
        evidence = (chain.initial_time_evidence,) + tuple(
            item.time_evidence for item in chain.mutations
        )
        all_evidence.extend(evidence)
        actuals = [
            item
            for item in chain.releases
            if item.actual_value is not None
            and item.stage is not EconomicReleaseStage.REVISION
        ]
        if actuals:
            initial_actuals += 1
            first_actual_by_event[chain.initial_release.logical_event_key] = (
                actuals[0]
            )
        else:
            missing_initial.append(chain.initial_release.logical_event_key)
        for mutation in chain.mutations:
            if mutation.revision_kind is not None:
                revision_total += 1
                revision_counts[mutation.revision_kind.value] += 1
    for time_item in all_evidence:
        basis_counts[time_item.availability_basis.value] += 1

    simultaneous_ok = True
    for chain in values:
        for mutation in chain.mutations:
            if (
                mutation.revision_kind
                is not EconomicReleaseRevisionKind.SIMULTANEOUS_PREVIOUS
            ):
                continue
            trigger = first_actual_by_event.get(
                cast(str, mutation.triggering_logical_event_key)
            )
            simultaneous_ok = simultaneous_ok and trigger is not None
            simultaneous_ok = simultaneous_ok and (
                trigger is not None
                and trigger.released_at_ns
                == mutation.time_evidence.actual_published_at_ns
                and trigger.reference_period_end_ns
                > chain.initial_release.reference_period_end_ns
                and chain.initial_release.reference_period
                in mutation.affected_reference_periods
            )

    previous_ok = True
    if len(values) <= MAX_ECONOMIC_CALENDAR_QUERY_EVENTS:
        query = query_economic_calendar_as_known(
            corpus,
            start_ns=corpus.coverage_start_ns,
            end_ns=corpus.coverage_end_ns,
            decision_at_ns=max(
                (item.available_at_ns for item in corpus.releases), default=0
            ),
            max_events=MAX_ECONOMIC_CALENDAR_QUERY_EVENTS,
        )
        states = {item.release.logical_event_key: item for item in query.events}
        for event_key, release in first_actual_by_event.items():
            candidates = [
                item
                for item in corpus.releases
                if item.series_id == release.series_id
                and item.reference_period_end_ns
                < release.reference_period_end_ns
                and item.actual_value is not None
            ]
            expected: EconomicCalendarReleaseV1 | None = None
            if candidates:
                prior_period = max(
                    item.reference_period_end_ns for item in candidates
                )
                eligible = [
                    item
                    for item in candidates
                    if item.reference_period_end_ns == prior_period
                    and item.available_at_ns < cast(int, release.released_at_ns)
                ]
                if eligible:
                    expected = max(
                        eligible,
                        key=lambda item: (
                            item.revision_sequence,
                            item.available_at_ns,
                        ),
                    )
            state = states.get(event_key)
            previous_ok = previous_ok and state is not None
            previous_ok = previous_ok and (
                state is not None and state.previous_as_known == expected
            )

    visibility_ok = True
    for chain in values:
        for index, release in enumerate(chain.releases):
            if release.available_at_ns:
                before = chain.as_known_at(release.available_at_ns - 1)
                expected_before = (
                    None if index == 0 else chain.releases[index - 1]
                )
                visibility_ok = visibility_ok and before == expected_before
            visibility_ok = visibility_ok and (
                chain.as_known_at(release.available_at_ns) == release
            )

    return EconomicReleaseVintageAuditV1(
        corpus_id=corpus.corpus_id,
        chain_ids=tuple(item.chain_id for item in values),
        event_count=len(values),
        release_count=len(reconstructed),
        initial_actual_count=initial_actuals,
        revision_count=revision_total,
        availability_basis_counts=tuple(basis_counts.items()),
        revision_kind_counts=tuple(revision_counts.items()),
        missing_initial_actual_event_keys=tuple(missing_initial),
        checks=(
            ("chain-replay", True),
            ("current-state-non-leakage", visibility_ok),
            ("previous-as-known", previous_ok),
            ("simultaneous-previous-revisions", simultaneous_ok),
        ),
        limitations=tuple(limitations),
    )
