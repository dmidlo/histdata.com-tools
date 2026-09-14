"""United States official macro-calendar backfill contracts and adapters.

The module keeps three claims separate: the first-party program inventory,
coverage measured from retained artifacts, and values reconstructed from one
exact official publication.  A reviewed archive route is never promoted to
historical coverage, and a current revised series is never treated as an
initial release.
"""

from __future__ import annotations

import base64
import calendar
import hashlib
import json
import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from histdatacom.market_context.contracts import (
    MarketContextKind,
    MarketContextPrecision,
    MarketContextSourceV1,
    canonical_contract_json,
)
from histdatacom.market_context.economic_calendar import (
    EconomicCalendarReleaseV1,
    EconomicEventFamily,
    EconomicReleaseStage,
    EconomicReleaseStatus,
    EconomicTimePrecision,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
    normalize_official_source_timestamp,
)
from histdatacom.runtime_contracts import JSONValue

US_OFFICIAL_PROGRAM_SCHEMA_VERSION = "histdatacom.us-official-program.v1"
US_BACKFILL_PROFILE_SCHEMA_VERSION = "histdatacom.us-backfill-profile.v1"
US_RELEASE_TRIPLET_SCHEMA_VERSION = "histdatacom.us-release-triplet.v1"
US_ARTIFACT_LOCK_SCHEMA_VERSION = "histdatacom.us-artifact-lock.v1"
US_PROGRAM_COVERAGE_SCHEMA_VERSION = "histdatacom.us-program-coverage.v1"
US_BACKFILL_AUDIT_SCHEMA_VERSION = "histdatacom.us-backfill-audit.v1"

US_BACKFILL_START_DATE = "2000-01-01"
MAX_US_PROGRAMS = 64
MAX_US_COVERAGE_ARTIFACTS = 100_000

_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TIME_RE = re.compile(r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")
_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9])[-+]?(?:\d+(?:\.\d*)?|\.\d+)")
_MONTHS = {
    name.lower(): index
    for index, name in enumerate(calendar.month_name)
    if name
}
_MONTHS.update(
    {
        name.rstrip(".").lower(): index
        for index, name in enumerate(calendar.month_abbr)
        if name
    }
)
_MONTHS["sept"] = 9
_G17_MONTH_RE = re.compile(
    r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
    r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|"
    r"oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?\b",
    re.IGNORECASE,
)


class UnitedStatesArchiveStrategy(str, Enum):
    """First-party evidence used to reconstruct publication vintages."""

    CONTEMPORANEOUS_RELEASE = "contemporaneous-release"
    OFFICIAL_VINTAGE_TABLE = "official-vintage-table"
    RELEASE_AND_VINTAGE_TABLE = "release-and-vintage-table"
    RETAINED_SNAPSHOT = "retained-snapshot"

    @classmethod
    def from_value(
        cls, value: str | UnitedStatesArchiveStrategy
    ) -> UnitedStatesArchiveStrategy:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported U.S. archive strategy") from exc


class UnitedStatesForecastStrategy(str, Enum):
    """Permitted official expectation evidence for a program."""

    UNAVAILABLE = "unavailable"
    FOMC_PROJECTION = "fomc-projection"
    OFFICIAL_PROFESSIONAL_SURVEY = "official-professional-survey"
    PRODUCER_OUTLOOK_SURVEY = "producer-outlook-survey"

    @classmethod
    def from_value(
        cls, value: str | UnitedStatesForecastStrategy
    ) -> UnitedStatesForecastStrategy:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported U.S. forecast strategy") from exc


class UnitedStatesCoverageGapReason(str, Enum):
    """Explicit reason a U.S. program occurrence is not qualified."""

    MISSING_SCHEDULE = "missing-schedule"
    MISSING_RELEASE_ARTIFACT = "missing-release-artifact"
    MISSING_INITIAL_VALUE = "missing-initial-value"
    MISSING_PREVIOUS_VALUE = "missing-previous-value"
    UNRESOLVED_REVISION = "unresolved-revision"
    IMPRECISE_RELEASE_TIME = "imprecise-release-time"
    NO_EVENT_FORECAST = "no-event-forecast"
    PROGRAM_NOT_YET_PUBLISHED = "program-not-yet-published"

    @classmethod
    def from_value(
        cls, value: str | UnitedStatesCoverageGapReason
    ) -> UnitedStatesCoverageGapReason:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value))
        except ValueError as exc:
            raise ValueError("unsupported U.S. coverage-gap reason") from exc

    @property
    def blocks_historical_qualification(self) -> bool:
        return self not in {
            UnitedStatesCoverageGapReason.NO_EVENT_FORECAST,
            UnitedStatesCoverageGapReason.PROGRAM_NOT_YET_PUBLISHED,
        }


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


def _https_uri(value: object, name: str) -> str:
    result = _required_text(value, name)
    parsed = urlparse(result)
    if parsed.scheme != "https" or not parsed.hostname or parsed.fragment:
        raise ValueError(f"{name} must be an HTTPS URI without a fragment")
    return result


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    return result


def _year_month(value: object, name: str) -> tuple[str, int, int]:
    result = _required_text(value, name)
    if re.fullmatch(r"\d{4}-\d{2}", result) is None:
        raise ValueError(f"{name} must be YYYY-MM")
    try:
        parsed = date.fromisoformat(f"{result}-01")
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM") from exc
    return result, parsed.year, parsed.month


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name).lower()
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return result


def _texts(
    values: Sequence[object], name: str, *, required: bool = False
) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item, name) for item in values}))
    if required and not result:
        raise ValueError(f"{name} must not be empty")
    if len(result) > 256:
        raise ValueError(f"{name} exceeds the item bound")
    return result


def _keys(values: Sequence[object], name: str) -> tuple[str, ...]:
    result = tuple(sorted({_key(item, name) for item in values}))
    if len(result) > MAX_US_PROGRAMS:
        raise ValueError(f"{name} exceeds the item bound")
    return result


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be numeric")
    result = float(cast(float, value))
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _count(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside the supported range")
    return value


def _mapping(value: object, name: str = "mapping") -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


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
class UnitedStatesOfficialProgramV1:
    """One legal-producer program required by the U.S. macro calendar."""

    program_key: str
    event_family: EconomicEventFamily
    indicator_id: str
    title: str
    legal_producer: str
    source_key: str
    archive_uri: str
    schedule_uri: str
    archive_start_date: str
    frequency: str
    scheduled_time_local: str | None
    time_precision: EconomicTimePrecision
    archive_strategy: UnitedStatesArchiveStrategy
    requires_previous_as_known: bool
    requires_revision_history: bool
    forecast_strategy: UnitedStatesForecastStrategy
    forecast_uri: str | None
    release_stages: tuple[EconomicReleaseStage, ...]
    limitations: tuple[str, ...]
    program_id: str = ""
    schema_version: str = US_OFFICIAL_PROGRAM_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != US_OFFICIAL_PROGRAM_SCHEMA_VERSION:
            raise ValueError("unsupported U.S. official-program schema")
        object.__setattr__(
            self, "program_key", _key(self.program_key, "program_key")
        )
        object.__setattr__(
            self,
            "event_family",
            EconomicEventFamily.from_value(self.event_family),
        )
        object.__setattr__(
            self, "indicator_id", _key(self.indicator_id, "indicator_id")
        )
        for name in ("title", "legal_producer", "frequency"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(
            self, "source_key", _key(self.source_key, "source_key")
        )
        object.__setattr__(
            self, "archive_uri", _https_uri(self.archive_uri, "archive_uri")
        )
        object.__setattr__(
            self, "schedule_uri", _https_uri(self.schedule_uri, "schedule_uri")
        )
        object.__setattr__(
            self,
            "archive_start_date",
            _iso_date(self.archive_start_date, "archive_start_date"),
        )
        local_time = _optional_text(self.scheduled_time_local)
        if local_time is not None and _TIME_RE.fullmatch(local_time) is None:
            raise ValueError("scheduled_time_local must be HH:MM")
        object.__setattr__(self, "scheduled_time_local", local_time)
        precision = EconomicTimePrecision.from_value(self.time_precision)
        if local_time is None and precision in {
            EconomicTimePrecision.EXACT_SECOND,
            EconomicTimePrecision.EXACT_MINUTE,
        }:
            raise ValueError(
                "exact release-time precision requires a local time"
            )
        object.__setattr__(self, "time_precision", precision)
        object.__setattr__(
            self,
            "archive_strategy",
            UnitedStatesArchiveStrategy.from_value(self.archive_strategy),
        )
        for name in (
            "requires_previous_as_known",
            "requires_revision_history",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        forecast_strategy = UnitedStatesForecastStrategy.from_value(
            self.forecast_strategy
        )
        forecast_uri = _optional_text(self.forecast_uri)
        if forecast_strategy is UnitedStatesForecastStrategy.UNAVAILABLE:
            if forecast_uri is not None:
                raise ValueError("unavailable forecasts cannot name a source")
        elif forecast_uri is None:
            raise ValueError("official forecast strategies require a source")
        object.__setattr__(self, "forecast_strategy", forecast_strategy)
        object.__setattr__(
            self,
            "forecast_uri",
            (
                None
                if forecast_uri is None
                else _https_uri(forecast_uri, "forecast_uri")
            ),
        )
        stages = tuple(
            sorted(
                {
                    EconomicReleaseStage.from_value(item)
                    for item in self.release_stages
                },
                key=lambda item: item.value,
            )
        )
        if not stages:
            raise ValueError("a U.S. program requires release stages")
        object.__setattr__(self, "release_stages", stages)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id("us-official-program", self.identity_payload())
        supplied = _optional_text(self.program_id)
        if supplied is not None and supplied != expected:
            raise ValueError("program_id does not match deterministic identity")
        object.__setattr__(self, "program_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "program_key": self.program_key,
            "event_family": self.event_family.value,
            "indicator_id": self.indicator_id,
            "title": self.title,
            "legal_producer": self.legal_producer,
            "source_key": self.source_key,
            "archive_uri": self.archive_uri,
            "schedule_uri": self.schedule_uri,
            "archive_start_date": self.archive_start_date,
            "frequency": self.frequency,
            "scheduled_time_local": self.scheduled_time_local,
            "time_precision": self.time_precision.value,
            "archive_strategy": self.archive_strategy.value,
            "requires_previous_as_known": self.requires_previous_as_known,
            "requires_revision_history": self.requires_revision_history,
            "forecast_strategy": self.forecast_strategy.value,
            "forecast_uri": self.forecast_uri,
            "release_stages": [item.value for item in self.release_stages],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "program_id": self.program_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> UnitedStatesOfficialProgramV1:
        return cls(
            program_key=str(data.get("program_key", "")),
            event_family=EconomicEventFamily.from_value(
                str(data.get("event_family", ""))
            ),
            indicator_id=str(data.get("indicator_id", "")),
            title=str(data.get("title", "")),
            legal_producer=str(data.get("legal_producer", "")),
            source_key=str(data.get("source_key", "")),
            archive_uri=str(data.get("archive_uri", "")),
            schedule_uri=str(data.get("schedule_uri", "")),
            archive_start_date=str(data.get("archive_start_date", "")),
            frequency=str(data.get("frequency", "")),
            scheduled_time_local=_optional_text(
                data.get("scheduled_time_local")
            ),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            archive_strategy=UnitedStatesArchiveStrategy.from_value(
                str(data.get("archive_strategy", ""))
            ),
            requires_previous_as_known=cast(
                bool, data.get("requires_previous_as_known")
            ),
            requires_revision_history=cast(
                bool, data.get("requires_revision_history")
            ),
            forecast_strategy=UnitedStatesForecastStrategy.from_value(
                str(data.get("forecast_strategy", ""))
            ),
            forecast_uri=_optional_text(data.get("forecast_uri")),
            release_stages=tuple(
                EconomicReleaseStage.from_value(str(item))
                for item in _sequence(
                    data.get("release_stages"), "release_stages"
                )
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            program_id=str(data.get("program_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class UnitedStatesBackfillProfileV1:
    """Complete required U.S. program and official-source inventory."""

    registry_id: str
    programs: tuple[UnitedStatesOfficialProgramV1, ...]
    required_program_keys: tuple[str, ...]
    limitations: tuple[str, ...]
    profile_id: str = ""
    schema_version: str = US_BACKFILL_PROFILE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != US_BACKFILL_PROFILE_SCHEMA_VERSION:
            raise ValueError("unsupported U.S. backfill-profile schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("registry_id is not an official registry identity")
        values = tuple(sorted(self.programs, key=lambda item: item.program_key))
        if (
            not values
            or len(values) > MAX_US_PROGRAMS
            or any(
                not isinstance(item, UnitedStatesOfficialProgramV1)
                for item in values
            )
        ):
            raise ValueError("U.S. backfill programs are invalid")
        if len({item.program_key for item in values}) != len(values):
            raise ValueError("U.S. backfill repeats a program key")
        required = _keys(self.required_program_keys, "required_program_keys")
        if set(required) != {item.program_key for item in values}:
            raise ValueError("required U.S. programs differ from the inventory")
        if {item.event_family for item in values} != set(EconomicEventFamily):
            raise ValueError("U.S. programs must cover every economic family")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "programs", values)
        object.__setattr__(self, "required_program_keys", required)
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id("us-backfill-profile", self.identity_payload())
        supplied = _optional_text(self.profile_id)
        if supplied is not None and supplied != expected:
            raise ValueError("profile_id does not match deterministic identity")
        object.__setattr__(self, "profile_id", expected)

    @property
    def by_key(self) -> Mapping[str, UnitedStatesOfficialProgramV1]:
        return MappingProxyType(
            {item.program_key: item for item in self.programs}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "programs": [item.to_dict() for item in self.programs],
            "required_program_keys": list(self.required_program_keys),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "profile_id": self.profile_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> UnitedStatesBackfillProfileV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            programs=tuple(
                UnitedStatesOfficialProgramV1.from_dict(
                    _mapping(item, "program")
                )
                for item in _sequence(data.get("programs"), "programs")
            ),
            required_program_keys=tuple(
                str(item)
                for item in _sequence(
                    data.get("required_program_keys"), "required_program_keys"
                )
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            profile_id=str(data.get("profile_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> UnitedStatesBackfillProfileV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("U.S. backfill profile is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "profile"))


@dataclass(frozen=True, slots=True)
class UnitedStatesReleaseTripletV1:
    """Raw-backed actual, previous-as-known, and revised-previous values."""

    program_key: str
    logical_event_key: str
    event_family: EconomicEventFamily
    reference_period: str
    previous_reference_period: str
    scheduled_lexical: str
    released_lexical: str
    source_timezone: str
    scheduled_for_ns: int
    released_at_ns: int
    actual_value: float
    actual_lexical: str
    previous_as_known_value: float
    previous_as_known_lexical: str
    revised_previous_value: float
    revised_previous_lexical: str
    unit: str
    transformation: str
    seasonality: str
    snapshot_id: str
    content_sha256: str
    source_locator: str
    release_time_locator: str
    actual_locator: str
    previous_locator: str
    revision_locator: str
    limitations: tuple[str, ...]
    triplet_id: str = ""
    schema_version: str = US_RELEASE_TRIPLET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != US_RELEASE_TRIPLET_SCHEMA_VERSION:
            raise ValueError("unsupported U.S. release-triplet schema")
        for name in ("program_key", "logical_event_key"):
            object.__setattr__(self, name, _key(getattr(self, name), name))
        object.__setattr__(
            self,
            "event_family",
            EconomicEventFamily.from_value(self.event_family),
        )
        for name in ("reference_period", "previous_reference_period"):
            value, _, _ = _year_month(getattr(self, name), name)
            object.__setattr__(self, name, value)
        if self.reference_period <= self.previous_reference_period:
            raise ValueError("previous reference period must precede current")
        for name in (
            "scheduled_lexical",
            "released_lexical",
            "source_timezone",
            "actual_lexical",
            "previous_as_known_lexical",
            "revised_previous_lexical",
            "unit",
            "transformation",
            "seasonality",
            "source_locator",
            "release_time_locator",
            "actual_locator",
            "previous_locator",
            "revision_locator",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        expected_scheduled = normalize_official_source_timestamp(
            self.scheduled_lexical,
            self.source_timezone,
            EconomicTimePrecision.EXACT_MINUTE,
        ).utc_ns
        expected_released = normalize_official_source_timestamp(
            self.released_lexical,
            self.source_timezone,
            EconomicTimePrecision.EXACT_MINUTE,
        ).utc_ns
        if (
            self.scheduled_for_ns != expected_scheduled
            or self.released_at_ns != expected_released
        ):
            raise ValueError(
                "U.S. triplet timestamp differs from lexical evidence"
            )
        if self.released_at_ns < self.scheduled_for_ns:
            raise ValueError("release precedes its scheduled time")
        for name in (
            "actual_value",
            "previous_as_known_value",
            "revised_previous_value",
        ):
            object.__setattr__(self, name, _finite(getattr(self, name), name))
        object.__setattr__(
            self, "snapshot_id", _required_text(self.snapshot_id, "snapshot_id")
        )
        if not self.snapshot_id.startswith("official-snapshot:sha256:"):
            raise ValueError("snapshot_id is not an official snapshot identity")
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        object.__setattr__(
            self,
            "source_locator",
            _https_uri(self.source_locator, "source_locator"),
        )
        object.__setattr__(
            self,
            "limitations",
            _texts(self.limitations, "limitations", required=True),
        )
        expected = _stable_id("us-release-triplet", self.identity_payload())
        supplied = _optional_text(self.triplet_id)
        if supplied is not None and supplied != expected:
            raise ValueError("triplet_id does not match deterministic identity")
        object.__setattr__(self, "triplet_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "program_key": self.program_key,
            "logical_event_key": self.logical_event_key,
            "event_family": self.event_family.value,
            "reference_period": self.reference_period,
            "previous_reference_period": self.previous_reference_period,
            "scheduled_lexical": self.scheduled_lexical,
            "released_lexical": self.released_lexical,
            "source_timezone": self.source_timezone,
            "scheduled_for_ns": self.scheduled_for_ns,
            "released_at_ns": self.released_at_ns,
            "actual_value": self.actual_value,
            "actual_lexical": self.actual_lexical,
            "previous_as_known_value": self.previous_as_known_value,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "revised_previous_value": self.revised_previous_value,
            "revised_previous_lexical": self.revised_previous_lexical,
            "unit": self.unit,
            "transformation": self.transformation,
            "seasonality": self.seasonality,
            "snapshot_id": self.snapshot_id,
            "content_sha256": self.content_sha256,
            "source_locator": self.source_locator,
            "release_time_locator": self.release_time_locator,
            "actual_locator": self.actual_locator,
            "previous_locator": self.previous_locator,
            "revision_locator": self.revision_locator,
            "limitations": list(self.limitations),
        }

    def normalization_payload(self) -> dict[str, JSONValue]:
        """Return parser output independent of when raw bytes were captured."""
        payload = self.identity_payload()
        payload.pop("snapshot_id")
        return payload

    @property
    def normalized_sha256(self) -> str:
        return hashlib.sha256(
            canonical_contract_json(self.normalization_payload()).encode(
                "utf-8"
            )
        ).hexdigest()

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "triplet_id": self.triplet_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> UnitedStatesReleaseTripletV1:
        return cls(
            program_key=str(data.get("program_key", "")),
            logical_event_key=str(data.get("logical_event_key", "")),
            event_family=EconomicEventFamily.from_value(
                str(data.get("event_family", ""))
            ),
            reference_period=str(data.get("reference_period", "")),
            previous_reference_period=str(
                data.get("previous_reference_period", "")
            ),
            scheduled_lexical=str(data.get("scheduled_lexical", "")),
            released_lexical=str(data.get("released_lexical", "")),
            source_timezone=str(data.get("source_timezone", "")),
            scheduled_for_ns=cast(int, data.get("scheduled_for_ns")),
            released_at_ns=cast(int, data.get("released_at_ns")),
            actual_value=cast(float, data.get("actual_value")),
            actual_lexical=str(data.get("actual_lexical", "")),
            previous_as_known_value=cast(
                float, data.get("previous_as_known_value")
            ),
            previous_as_known_lexical=str(
                data.get("previous_as_known_lexical", "")
            ),
            revised_previous_value=cast(
                float, data.get("revised_previous_value")
            ),
            revised_previous_lexical=str(
                data.get("revised_previous_lexical", "")
            ),
            unit=str(data.get("unit", "")),
            transformation=str(data.get("transformation", "")),
            seasonality=str(data.get("seasonality", "")),
            snapshot_id=str(data.get("snapshot_id", "")),
            content_sha256=str(data.get("content_sha256", "")),
            source_locator=str(data.get("source_locator", "")),
            release_time_locator=str(data.get("release_time_locator", "")),
            actual_locator=str(data.get("actual_locator", "")),
            previous_locator=str(data.get("previous_locator", "")),
            revision_locator=str(data.get("revision_locator", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            triplet_id=str(data.get("triplet_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class UnitedStatesArtifactLockV1:
    """Pinned first-party raw artifact and expected normalized triplet."""

    program_key: str
    artifact_uri: str
    content_sha256: str
    expected_reference_period: str
    expected_actual_value: float
    expected_previous_as_known_value: float
    expected_revised_previous_value: float
    expected_release_lexical: str
    normalized_sha256: str
    lock_id: str = ""
    schema_version: str = US_ARTIFACT_LOCK_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != US_ARTIFACT_LOCK_SCHEMA_VERSION:
            raise ValueError("unsupported U.S. artifact-lock schema")
        object.__setattr__(
            self, "program_key", _key(self.program_key, "program_key")
        )
        object.__setattr__(
            self,
            "artifact_uri",
            _https_uri(self.artifact_uri, "artifact_uri"),
        )
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        period, _, _ = _year_month(
            self.expected_reference_period, "expected_reference_period"
        )
        object.__setattr__(self, "expected_reference_period", period)
        for name in (
            "expected_actual_value",
            "expected_previous_as_known_value",
            "expected_revised_previous_value",
        ):
            object.__setattr__(self, name, _finite(getattr(self, name), name))
        object.__setattr__(
            self,
            "expected_release_lexical",
            _required_text(
                self.expected_release_lexical, "expected_release_lexical"
            ),
        )
        object.__setattr__(
            self,
            "normalized_sha256",
            _sha256(self.normalized_sha256, "normalized_sha256"),
        )
        expected = _stable_id("us-artifact-lock", self.identity_payload())
        supplied = _optional_text(self.lock_id)
        if supplied is not None and supplied != expected:
            raise ValueError("lock_id does not match deterministic identity")
        object.__setattr__(self, "lock_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "program_key": self.program_key,
            "artifact_uri": self.artifact_uri,
            "content_sha256": self.content_sha256,
            "expected_reference_period": self.expected_reference_period,
            "expected_actual_value": self.expected_actual_value,
            "expected_previous_as_known_value": (
                self.expected_previous_as_known_value
            ),
            "expected_revised_previous_value": self.expected_revised_previous_value,
            "expected_release_lexical": self.expected_release_lexical,
            "normalized_sha256": self.normalized_sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "lock_id": self.lock_id}

    def verify(self, triplet: UnitedStatesReleaseTripletV1) -> None:
        if not isinstance(triplet, UnitedStatesReleaseTripletV1):
            raise TypeError("artifact lock requires a U.S. release triplet")
        if (
            triplet.program_key != self.program_key
            or triplet.source_locator != self.artifact_uri
            or triplet.content_sha256 != self.content_sha256
            or triplet.reference_period != self.expected_reference_period
            or triplet.actual_value != self.expected_actual_value
            or triplet.previous_as_known_value
            != self.expected_previous_as_known_value
            or triplet.revised_previous_value
            != self.expected_revised_previous_value
            or triplet.released_lexical != self.expected_release_lexical
            or triplet.normalized_sha256 != self.normalized_sha256
        ):
            raise ValueError("U.S. artifact lock differs from replayed triplet")


@dataclass(frozen=True, slots=True)
class UnitedStatesProgramCoverageV1:
    """Measured 2000-present coverage for one official U.S. program."""

    program_key: str
    window_start_date: str
    window_end_date: str
    expected_occurrence_count: int
    schedule_count: int
    initial_actual_count: int
    previous_as_known_count: int
    revision_count: int
    exact_minute_count: int
    forecast_count: int
    artifact_sha256s: tuple[str, ...]
    gap_reasons: tuple[UnitedStatesCoverageGapReason, ...]
    notes: tuple[str, ...]
    coverage_id: str = ""
    schema_version: str = US_PROGRAM_COVERAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != US_PROGRAM_COVERAGE_SCHEMA_VERSION:
            raise ValueError("unsupported U.S. program-coverage schema")
        object.__setattr__(
            self, "program_key", _key(self.program_key, "program_key")
        )
        start = _iso_date(self.window_start_date, "window_start_date")
        end = _iso_date(self.window_end_date, "window_end_date")
        if end < start:
            raise ValueError("U.S. coverage window end precedes start")
        object.__setattr__(self, "window_start_date", start)
        object.__setattr__(self, "window_end_date", end)
        expected = _count(
            self.expected_occurrence_count,
            "expected_occurrence_count",
            MAX_US_COVERAGE_ARTIFACTS,
        )
        if expected == 0:
            raise ValueError(
                "U.S. program coverage requires expected occurrences"
            )
        for name in (
            "schedule_count",
            "initial_actual_count",
            "previous_as_known_count",
            "exact_minute_count",
        ):
            value = _count(getattr(self, name), name, MAX_US_COVERAGE_ARTIFACTS)
            if value > expected:
                raise ValueError(f"{name} exceeds expected occurrences")
        for name in ("revision_count", "forecast_count"):
            _count(getattr(self, name), name, MAX_US_COVERAGE_ARTIFACTS)
        artifacts = tuple(
            sorted(
                {
                    _sha256(item, "artifact_sha256s")
                    for item in self.artifact_sha256s
                }
            )
        )
        if len(artifacts) > MAX_US_COVERAGE_ARTIFACTS:
            raise ValueError("U.S. coverage artifact count exceeds the bound")
        gaps = tuple(
            sorted(
                {
                    UnitedStatesCoverageGapReason.from_value(item)
                    for item in self.gap_reasons
                },
                key=lambda item: item.value,
            )
        )
        object.__setattr__(self, "artifact_sha256s", artifacts)
        object.__setattr__(self, "gap_reasons", gaps)
        object.__setattr__(
            self, "notes", _texts(self.notes, "notes", required=True)
        )
        expected_id = _stable_id("us-program-coverage", self.identity_payload())
        supplied = _optional_text(self.coverage_id)
        if supplied is not None and supplied != expected_id:
            raise ValueError(
                "coverage_id does not match deterministic identity"
            )
        object.__setattr__(self, "coverage_id", expected_id)

    @property
    def blocking_gap_reasons(self) -> tuple[UnitedStatesCoverageGapReason, ...]:
        return tuple(
            item
            for item in self.gap_reasons
            if item.blocks_historical_qualification
        )

    def is_complete_for(self, program: UnitedStatesOfficialProgramV1) -> bool:
        if self.program_key != program.program_key:
            raise ValueError("coverage and U.S. program keys differ")
        expected = self.expected_occurrence_count
        minimum_artifacts = (
            1
            if program.archive_strategy
            is UnitedStatesArchiveStrategy.OFFICIAL_VINTAGE_TABLE
            else expected
        )
        return (
            self.window_start_date == US_BACKFILL_START_DATE
            and self.schedule_count == expected
            and self.initial_actual_count == expected
            and (
                not program.requires_previous_as_known
                or self.previous_as_known_count == expected
            )
            and (
                not program.requires_revision_history or self.revision_count > 0
            )
            and (
                program.time_precision
                not in {
                    EconomicTimePrecision.EXACT_MINUTE,
                    EconomicTimePrecision.EXACT_SECOND,
                }
                or self.exact_minute_count == expected
            )
            and len(self.artifact_sha256s) >= minimum_artifacts
            and not self.blocking_gap_reasons
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "program_key": self.program_key,
            "window_start_date": self.window_start_date,
            "window_end_date": self.window_end_date,
            "expected_occurrence_count": self.expected_occurrence_count,
            "schedule_count": self.schedule_count,
            "initial_actual_count": self.initial_actual_count,
            "previous_as_known_count": self.previous_as_known_count,
            "revision_count": self.revision_count,
            "exact_minute_count": self.exact_minute_count,
            "forecast_count": self.forecast_count,
            "artifact_sha256s": list(self.artifact_sha256s),
            "gap_reasons": [item.value for item in self.gap_reasons],
            "notes": list(self.notes),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "coverage_id": self.coverage_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> UnitedStatesProgramCoverageV1:
        return cls(
            program_key=str(data.get("program_key", "")),
            window_start_date=str(data.get("window_start_date", "")),
            window_end_date=str(data.get("window_end_date", "")),
            expected_occurrence_count=cast(
                int, data.get("expected_occurrence_count")
            ),
            schedule_count=cast(int, data.get("schedule_count")),
            initial_actual_count=cast(int, data.get("initial_actual_count")),
            previous_as_known_count=cast(
                int, data.get("previous_as_known_count")
            ),
            revision_count=cast(int, data.get("revision_count")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            forecast_count=cast(int, data.get("forecast_count")),
            artifact_sha256s=tuple(
                str(item)
                for item in _sequence(
                    data.get("artifact_sha256s"), "artifact_sha256s"
                )
            ),
            gap_reasons=tuple(
                UnitedStatesCoverageGapReason.from_value(str(item))
                for item in _sequence(data.get("gap_reasons"), "gap_reasons")
            ),
            notes=tuple(
                str(item) for item in _sequence(data.get("notes"), "notes")
            ),
            coverage_id=str(data.get("coverage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class UnitedStatesBackfillAuditV1:
    """Deterministic closure audit for an evidence-backed U.S. backfill."""

    profile_id: str
    window_end_date: str
    program_count: int
    family_counts: tuple[tuple[str, int], ...]
    expected_occurrence_count: int
    schedule_count: int
    initial_actual_count: int
    previous_as_known_count: int
    revision_count: int
    exact_minute_count: int
    forecast_count: int
    incomplete_program_keys: tuple[str, ...]
    blocking_gap_program_keys: tuple[str, ...]
    explicit_no_forecast_program_keys: tuple[str, ...]
    checks: tuple[tuple[str, bool], ...]
    audit_id: str = ""
    schema_version: str = US_BACKFILL_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != US_BACKFILL_AUDIT_SCHEMA_VERSION:
            raise ValueError("unsupported U.S. backfill-audit schema")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("profile_id is invalid")
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(
            self,
            "window_end_date",
            _iso_date(self.window_end_date, "window_end_date"),
        )
        _count(self.program_count, "program_count", MAX_US_PROGRAMS)
        families = tuple(
            sorted((str(key), int(value)) for key, value in self.family_counts)
        )
        if (
            {key for key, _ in families}
            != {item.value for item in EconomicEventFamily}
            or any(value <= 0 for _, value in families)
            or sum(value for _, value in families) != self.program_count
        ):
            raise ValueError("U.S. audit family counts are inconsistent")
        object.__setattr__(self, "family_counts", families)
        for name in (
            "expected_occurrence_count",
            "schedule_count",
            "initial_actual_count",
            "previous_as_known_count",
            "revision_count",
            "exact_minute_count",
            "forecast_count",
        ):
            _count(getattr(self, name), name, MAX_US_COVERAGE_ARTIFACTS)
        for name in (
            "incomplete_program_keys",
            "blocking_gap_program_keys",
            "explicit_no_forecast_program_keys",
        ):
            object.__setattr__(self, name, _keys(getattr(self, name), name))
        checks = tuple(
            sorted((str(name), value) for name, value in self.checks)
        )
        if (
            not checks
            or len({name for name, _ in checks}) != len(checks)
            or any(
                not name or not isinstance(value, bool)
                for name, value in checks
            )
        ):
            raise ValueError("U.S. audit requires unique boolean checks")
        object.__setattr__(self, "checks", checks)
        expected = _stable_id("us-backfill-audit", self.identity_payload())
        supplied = _optional_text(self.audit_id)
        if supplied is not None and supplied != expected:
            raise ValueError("audit_id does not match deterministic identity")
        object.__setattr__(self, "audit_id", expected)

    @property
    def passed(self) -> bool:
        return all(value for _, value in self.checks)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "profile_id": self.profile_id,
            "window_end_date": self.window_end_date,
            "program_count": self.program_count,
            "family_counts": [
                {"family": key, "count": value}
                for key, value in self.family_counts
            ],
            "expected_occurrence_count": self.expected_occurrence_count,
            "schedule_count": self.schedule_count,
            "initial_actual_count": self.initial_actual_count,
            "previous_as_known_count": self.previous_as_known_count,
            "revision_count": self.revision_count,
            "exact_minute_count": self.exact_minute_count,
            "forecast_count": self.forecast_count,
            "incomplete_program_keys": list(self.incomplete_program_keys),
            "blocking_gap_program_keys": list(self.blocking_gap_program_keys),
            "explicit_no_forecast_program_keys": list(
                self.explicit_no_forecast_program_keys
            ),
            "checks": [
                {"name": name, "passed": value} for name, value in self.checks
            ],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "audit_id": self.audit_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> UnitedStatesBackfillAuditV1:
        return cls(
            profile_id=str(data.get("profile_id", "")),
            window_end_date=str(data.get("window_end_date", "")),
            program_count=cast(int, data.get("program_count")),
            family_counts=tuple(
                (
                    str(_mapping(item, "family count").get("family", "")),
                    cast(int, _mapping(item, "family count").get("count")),
                )
                for item in _sequence(
                    data.get("family_counts"), "family_counts"
                )
            ),
            expected_occurrence_count=cast(
                int, data.get("expected_occurrence_count")
            ),
            schedule_count=cast(int, data.get("schedule_count")),
            initial_actual_count=cast(int, data.get("initial_actual_count")),
            previous_as_known_count=cast(
                int, data.get("previous_as_known_count")
            ),
            revision_count=cast(int, data.get("revision_count")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            forecast_count=cast(int, data.get("forecast_count")),
            incomplete_program_keys=tuple(
                str(item)
                for item in _sequence(
                    data.get("incomplete_program_keys"),
                    "incomplete_program_keys",
                )
            ),
            blocking_gap_program_keys=tuple(
                str(item)
                for item in _sequence(
                    data.get("blocking_gap_program_keys"),
                    "blocking_gap_program_keys",
                )
            ),
            explicit_no_forecast_program_keys=tuple(
                str(item)
                for item in _sequence(
                    data.get("explicit_no_forecast_program_keys"),
                    "explicit_no_forecast_program_keys",
                )
            ),
            checks=tuple(
                (
                    str(_mapping(item, "check").get("name", "")),
                    cast(bool, _mapping(item, "check").get("passed")),
                )
                for item in _sequence(data.get("checks"), "checks")
            ),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> UnitedStatesBackfillAuditV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("U.S. backfill audit is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "audit"))


def audit_united_states_backfill(
    profile: UnitedStatesBackfillProfileV1,
    coverages: Sequence[UnitedStatesProgramCoverageV1],
) -> UnitedStatesBackfillAuditV1:
    """Quantify every U.S. program and fail closed on missing evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("U.S. audit requires a v1 profile")
    values = tuple(coverages)
    if any(
        not isinstance(item, UnitedStatesProgramCoverageV1) for item in values
    ):
        raise TypeError("U.S. audit requires v1 coverage slices")
    by_key = {item.program_key: item for item in values}
    if len(by_key) != len(values):
        raise ValueError("U.S. audit repeats program coverage")
    expected_keys = set(profile.required_program_keys)
    incomplete = tuple(
        sorted(
            key
            for key in expected_keys
            if key not in by_key
            or not by_key[key].is_complete_for(profile.by_key[key])
        )
    )
    blocking = tuple(
        sorted(item.program_key for item in values if item.blocking_gap_reasons)
    )
    end_dates = {item.window_end_date for item in values}
    common_end = (
        next(iter(end_dates)) if len(end_dates) == 1 else US_BACKFILL_START_DATE
    )
    no_forecast = tuple(
        sorted(
            item.program_key
            for item in profile.programs
            if item.forecast_strategy
            is UnitedStatesForecastStrategy.UNAVAILABLE
        )
    )
    family_counts = Counter(
        item.event_family.value for item in profile.programs
    )
    return UnitedStatesBackfillAuditV1(
        profile_id=profile.profile_id,
        window_end_date=common_end,
        program_count=len(profile.programs),
        family_counts=tuple(sorted(family_counts.items())),
        expected_occurrence_count=sum(
            item.expected_occurrence_count for item in values
        ),
        schedule_count=sum(item.schedule_count for item in values),
        initial_actual_count=sum(item.initial_actual_count for item in values),
        previous_as_known_count=sum(
            item.previous_as_known_count for item in values
        ),
        revision_count=sum(item.revision_count for item in values),
        exact_minute_count=sum(item.exact_minute_count for item in values),
        forecast_count=sum(item.forecast_count for item in values),
        incomplete_program_keys=incomplete,
        blocking_gap_program_keys=blocking,
        explicit_no_forecast_program_keys=no_forecast,
        checks=(
            ("all-required-programs-measured", set(by_key) == expected_keys),
            ("all-twelve-families-covered", len(family_counts) == 12),
            ("common-2000-present-window", len(end_dates) == 1),
            ("historical-programs-qualified", not incomplete),
            ("no-blocking-gaps", not blocking),
            (
                "unsupported-event-forecasts-explicit",
                all(
                    UnitedStatesCoverageGapReason.NO_EVENT_FORECAST
                    in by_key[item.program_key].gap_reasons
                    for item in profile.programs
                    if item.forecast_strategy
                    is UnitedStatesForecastStrategy.UNAVAILABLE
                    and item.program_key in by_key
                ),
            ),
        ),
    )


def _program(
    program_key: str,
    event_family: EconomicEventFamily,
    indicator_id: str,
    title: str,
    legal_producer: str,
    source_key: str,
    archive_uri: str,
    schedule_uri: str,
    *,
    archive_start_date: str = US_BACKFILL_START_DATE,
    frequency: str = "monthly",
    scheduled_time_local: str | None = "08:30",
    time_precision: EconomicTimePrecision = EconomicTimePrecision.EXACT_MINUTE,
    archive_strategy: UnitedStatesArchiveStrategy = (
        UnitedStatesArchiveStrategy.CONTEMPORANEOUS_RELEASE
    ),
    requires_previous_as_known: bool = True,
    requires_revision_history: bool = True,
    forecast_strategy: UnitedStatesForecastStrategy = (
        UnitedStatesForecastStrategy.UNAVAILABLE
    ),
    forecast_uri: str | None = None,
    release_stages: tuple[EconomicReleaseStage, ...] = (
        EconomicReleaseStage.INITIAL,
        EconomicReleaseStage.REVISION,
    ),
    limitations: tuple[str, ...] = (
        "A current revised series cannot establish a historical initial value.",
    ),
) -> UnitedStatesOfficialProgramV1:
    return UnitedStatesOfficialProgramV1(
        program_key=program_key,
        event_family=event_family,
        indicator_id=indicator_id,
        title=title,
        legal_producer=legal_producer,
        source_key=source_key,
        archive_uri=archive_uri,
        schedule_uri=schedule_uri,
        archive_start_date=archive_start_date,
        frequency=frequency,
        scheduled_time_local=scheduled_time_local,
        time_precision=time_precision,
        archive_strategy=archive_strategy,
        requires_previous_as_known=requires_previous_as_known,
        requires_revision_history=requires_revision_history,
        forecast_strategy=forecast_strategy,
        forecast_uri=forecast_uri,
        release_stages=release_stages,
        limitations=limitations,
    )


def built_in_united_states_backfill_profile(
    registry: OfficialSourceRegistryV1,
) -> UnitedStatesBackfillProfileV1:
    """Build the required legal-producer map for the U.S. backfill."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("U.S. backfill profile requires a v1 source registry")
    bls_schedule = "https://www.bls.gov/schedule/"
    bea_archive = "https://www.bea.gov/news/archive"
    bea_schedule = "https://www.bea.gov/news/schedule"
    census_schedule = (
        "https://www.census.gov/economic-indicators/calendar-listview.html"
    )
    fomc_archive = (
        "https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm"
    )
    fomc_calendar = (
        "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    )
    spf_uri = (
        "https://www.philadelphiafed.org/surveys-and-data/"
        "real-time-data-research/survey-of-professional-forecasters"
    )
    no_monthly_consensus = (
        (
            "No official event-level consensus is published; leave the "
            "forecast missing instead of substituting a proprietary survey."
        ),
    )
    programs = (
        _program(
            "us.fomc.decision",
            EconomicEventFamily.MONETARY_POLICY,
            "fomc-policy-decision",
            "FOMC policy decision and statement",
            "Federal Open Market Committee",
            "us.frb.fomc",
            fomc_archive,
            fomc_calendar,
            frequency="irregular",
            scheduled_time_local=None,
            time_precision=EconomicTimePrecision.INFERRED_BOUNDED,
            requires_revision_history=False,
            limitations=(
                "Announcement times changed by era and must come from each statement or contemporaneous schedule.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.fomc.minutes",
            EconomicEventFamily.MONETARY_POLICY,
            "fomc-minutes",
            "FOMC meeting minutes",
            "Federal Open Market Committee",
            "us.frb.fomc",
            fomc_archive,
            fomc_calendar,
            frequency="irregular",
            scheduled_time_local=None,
            time_precision=EconomicTimePrecision.SCHEDULED_ONLY,
            requires_previous_as_known=False,
            requires_revision_history=False,
            release_stages=(EconomicReleaseStage.INITIAL,),
            limitations=(
                "The minutes release lag changed in December 2004 and is part of the event identity.",
                "Minutes are documentary events, not numeric event-consensus observations.",
            ),
        ),
        _program(
            "us.fomc.sep",
            EconomicEventFamily.MONETARY_POLICY,
            "fomc-summary-economic-projections",
            "FOMC Summary of Economic Projections",
            "Federal Open Market Committee",
            "us.frb.fomc",
            fomc_archive,
            fomc_calendar,
            archive_start_date="2007-10-31",
            frequency="quarterly-irregular",
            scheduled_time_local=None,
            time_precision=EconomicTimePrecision.SCHEDULED_ONLY,
            requires_previous_as_known=False,
            requires_revision_history=False,
            forecast_strategy=UnitedStatesForecastStrategy.FOMC_PROJECTION,
            forecast_uri=fomc_archive,
            release_stages=(EconomicReleaseStage.INITIAL,),
            limitations=(
                "SEP began in October 2007 and its publication timing changed in 2011 and 2020.",
                "SEP projections are annual or quarterly policy projections, not monthly release consensus.",
            ),
        ),
        _program(
            "us.bls.cpi",
            EconomicEventFamily.INFLATION_PRICES,
            "consumer-price-index",
            "Consumer Price Index",
            "U.S. Bureau of Labor Statistics",
            "us.bls.cpi",
            "https://www.bls.gov/bls/news-release/cpi.htm",
            bls_schedule,
            limitations=(
                "Use the complete occurrence-specific CPI-U all-items archive manifest for initial, previous-as-known, and revised-previous values.",
                "October 2025 was not published; November is a two-month change and December has no comparable revised-November value.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.bls.ppi",
            EconomicEventFamily.INFLATION_PRICES,
            "producer-price-index",
            "Producer Price Index",
            "U.S. Bureau of Labor Statistics",
            "us.bls.ppi",
            "https://www.bls.gov/bls/news-release/ppi.htm",
            bls_schedule,
            limitations=(
                "Use the complete occurrence-specific finished-goods and final-demand PPI archive manifest for initial, previous-as-known, and revised-previous values.",
                "October 2025 was not published; the November release uses a subsequently calculated October index while previous-as-known remains September.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.bls.employment-situation",
            EconomicEventFamily.LABOUR_MARKET,
            "employment-situation",
            "Employment Situation",
            "U.S. Bureau of Labor Statistics",
            "us.bls.employment-situation",
            "https://www.bls.gov/bls/news-release/empsit.htm",
            bls_schedule,
            limitations=(
                "Use the complete occurrence-specific Employment Situation archive manifest for CPS unemployment, CES payrolls, and CES earnings.",
                "Household and establishment surveys remain distinct within the release package; the January 2010 earnings-lineage break stays explicit.",
                "October 2025 was not published; November retains September as the previous published occurrence.",
                "Benchmark revisions must not rewrite earlier payroll vintages.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.bls.jolts",
            EconomicEventFamily.LABOUR_MARKET,
            "job-openings-labor-turnover",
            "Job Openings and Labor Turnover Survey",
            "U.S. Bureau of Labor Statistics",
            "us.bls.public-data",
            "https://www.bls.gov/bls/news-release/jolts.htm",
            bls_schedule,
            archive_start_date="2002-07-30",
            scheduled_time_local="10:00",
            limitations=(
                "The JOLTS release series begins after the requested window start.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.dol.initial-claims",
            EconomicEventFamily.LABOUR_MARKET,
            "weekly-unemployment-insurance-claims",
            "Unemployment Insurance Weekly Claims",
            "U.S. Department of Labor Employment and Training Administration",
            "us.dol.eta-unemployment-insurance",
            "https://oui.doleta.gov/unemploy/claims.asp",
            "https://www.dol.gov/ui/data.pdf",
            frequency="weekly",
            limitations=(
                "State administrative revisions and pandemic programs require explicit series identity.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.bls.productivity-costs",
            EconomicEventFamily.LABOUR_MARKET,
            "productivity-and-costs",
            "Productivity and Costs",
            "U.S. Bureau of Labor Statistics",
            "us.bls.public-data",
            "https://www.bls.gov/bls/news-release/prod2.htm",
            bls_schedule,
            frequency="quarterly",
            release_stages=(
                EconomicReleaseStage.PRELIMINARY,
                EconomicReleaseStage.FINAL,
                EconomicReleaseStage.REVISION,
            ),
            limitations=(
                "Preliminary and revised quarterly estimates are separate release stages.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.bea.gdp",
            EconomicEventFamily.GDP_NATIONAL_ACCOUNTS,
            "gross-domestic-product",
            "Gross Domestic Product",
            "U.S. Bureau of Economic Analysis",
            "us.bea.api",
            bea_archive,
            bea_schedule,
            frequency="quarterly",
            archive_strategy=UnitedStatesArchiveStrategy.RELEASE_AND_VINTAGE_TABLE,
            forecast_strategy=(
                UnitedStatesForecastStrategy.OFFICIAL_PROFESSIONAL_SURVEY
            ),
            forecast_uri=spf_uri,
            release_stages=(
                EconomicReleaseStage.ADVANCE,
                EconomicReleaseStage.SECOND,
                EconomicReleaseStage.FINAL,
                EconomicReleaseStage.REVISION,
            ),
            limitations=(
                "Advance, second, third, annual-update, and comprehensive-update vintages remain distinct.",
                "SPF is quarterly and cannot be projected into a monthly event consensus.",
            ),
        ),
        _program(
            "us.bea.personal-income-outlays",
            EconomicEventFamily.GDP_NATIONAL_ACCOUNTS,
            "personal-income-and-outlays",
            "Personal Income and Outlays",
            "U.S. Bureau of Economic Analysis",
            "us.bea.api",
            bea_archive,
            bea_schedule,
            archive_strategy=UnitedStatesArchiveStrategy.RELEASE_AND_VINTAGE_TABLE,
            limitations=(
                "Income, spending, headline PCE, and core PCE retain separate concepts and transformations.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.census.retail-sales",
            EconomicEventFamily.RETAIL_CONSUMPTION,
            "advance-monthly-retail-sales",
            "Advance Monthly Sales for Retail and Food Services",
            "U.S. Census Bureau",
            "us.census.eits",
            "https://www.census.gov/retail/marts/historic_releases.html",
            census_schedule,
            limitations=(
                "Total, ex-autos, ex-gas, and control-group measures remain separate series.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.frb.industrial-production",
            EconomicEventFamily.INDUSTRIAL_PRODUCTION,
            "industrial-production-capacity-utilization",
            "Industrial Production and Capacity Utilization",
            "Board of Governors of the Federal Reserve System",
            "us.frb.industrial-production",
            "https://www.federalreserve.gov/releases/g17/",
            "https://www.federalreserve.gov/releases/g17/release_dates.htm",
            scheduled_time_local="09:15",
            limitations=(
                "Annual benchmark, classification, and base changes are immutable vintage events.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.census.durable-goods",
            EconomicEventFamily.OTHER_OFFICIAL,
            "advance-durable-goods-orders",
            "Advance Report on Durable Goods",
            "U.S. Census Bureau",
            "us.census.eits",
            "https://www.census.gov/manufacturing/m3/historical_data/index.html",
            census_schedule,
            limitations=(
                "Headline durable orders and core capital-goods orders remain separate concepts.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.bea-census.international-trade",
            EconomicEventFamily.TRADE_EXTERNAL,
            "international-trade-goods-services",
            "U.S. International Trade in Goods and Services",
            "U.S. Census Bureau and U.S. Bureau of Economic Analysis",
            "us.census.eits",
            "https://www.census.gov/foreign-trade/Press-Release/archive.html",
            census_schedule,
            limitations=(
                "Joint Census-BEA publication identity is retained; goods and services are not conflated.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.census.housing-starts-permits",
            EconomicEventFamily.HOUSING,
            "new-residential-construction",
            "New Residential Construction",
            "U.S. Census Bureau and U.S. Department of Housing and Urban Development",
            "us.census.eits",
            "https://www.census.gov/construction/nrc/data/releases.html",
            census_schedule,
            limitations=(
                "Starts, permits, and completions retain separate measure identity.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.census.new-home-sales",
            EconomicEventFamily.HOUSING,
            "new-residential-sales",
            "New Residential Sales",
            "U.S. Census Bureau and U.S. Department of Housing and Urban Development",
            "us.census.eits",
            "https://www.census.gov/construction/nrs/historical_data/index.html",
            census_schedule,
            scheduled_time_local="10:00",
            limitations=(
                "Initial estimates and later survey revisions remain separate vintages.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.philadelphia-fed.mbos",
            EconomicEventFamily.CONFIDENCE_SURVEY,
            "manufacturing-business-outlook-survey",
            "Manufacturing Business Outlook Survey",
            "Federal Reserve Bank of Philadelphia",
            "us.frb.philadelphia-surveys",
            "https://www.philadelphiafed.org/surveys-and-data/mbos-archives",
            (
                "https://www.philadelphiafed.org/surveys-and-data/"
                "regional-economic-analysis/manufacturing-business-outlook-survey"
            ),
            scheduled_time_local="08:30",
            forecast_strategy=(
                UnitedStatesForecastStrategy.PRODUCER_OUTLOOK_SURVEY
            ),
            forecast_uri=(
                "https://www.philadelphiafed.org/surveys-and-data/"
                "regional-economic-analysis/manufacturing-business-outlook-survey"
            ),
            limitations=(
                "MBOS is a Third District diffusion survey, not a national output statistic.",
                "Current and six-month-future indexes are distinct survey concepts.",
            ),
        ),
        _program(
            "us.philadelphia-fed.spf",
            EconomicEventFamily.CONFIDENCE_SURVEY,
            "survey-professional-forecasters",
            "Survey of Professional Forecasters",
            "Federal Reserve Bank of Philadelphia",
            "us.frb.philadelphia-surveys",
            spf_uri,
            spf_uri,
            frequency="quarterly",
            scheduled_time_local=None,
            time_precision=EconomicTimePrecision.DATE_ONLY,
            archive_strategy=UnitedStatesArchiveStrategy.OFFICIAL_VINTAGE_TABLE,
            requires_previous_as_known=False,
            forecast_strategy=(
                UnitedStatesForecastStrategy.OFFICIAL_PROFESSIONAL_SURVEY
            ),
            forecast_uri=spf_uri,
            limitations=(
                "SPF survey vintages and respondent panels must remain point in time.",
                "Quarterly SPF aggregates are not monthly event-consensus values.",
            ),
        ),
        _program(
            "us.frb.money-stock",
            EconomicEventFamily.MONEY_CREDIT,
            "money-stock-measures",
            "Money Stock Measures",
            "Board of Governors of the Federal Reserve System",
            "us.frb.fomc",
            "https://www.federalreserve.gov/releases/h6/",
            "https://www.federalreserve.gov/releases/h6/",
            frequency="weekly-monthly",
            scheduled_time_local=None,
            time_precision=EconomicTimePrecision.SCHEDULED_ONLY,
            limitations=(
                "Weekly and monthly frequencies and the 2021 publication redesign remain separate eras.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.treasury.monthly-statement",
            EconomicEventFamily.FISCAL,
            "monthly-treasury-statement",
            "Monthly Treasury Statement",
            "U.S. Department of the Treasury",
            "us.treasury.fiscal-data",
            "https://fiscaldata.treasury.gov/datasets/monthly-treasury-statement/",
            "https://fiscaldata.treasury.gov/datasets/monthly-treasury-statement/",
            scheduled_time_local=None,
            time_precision=EconomicTimePrecision.DATE_ONLY,
            archive_strategy=UnitedStatesArchiveStrategy.OFFICIAL_VINTAGE_TABLE,
            limitations=(
                "Fiscal-period revisions and classification changes require vintage metadata.",
                *no_monthly_consensus,
            ),
        ),
        _program(
            "us.census.manufacturing-trade-inventories",
            EconomicEventFamily.OTHER_OFFICIAL,
            "manufacturing-trade-inventories-sales",
            "Manufacturing and Trade Inventories and Sales",
            "U.S. Census Bureau",
            "us.census.eits",
            "https://www.census.gov/mtis/www/historical/index.html",
            census_schedule,
            scheduled_time_local="10:00",
            limitations=(
                "Inventories, sales, and inventory-sales ratios remain separate concepts.",
                *no_monthly_consensus,
            ),
        ),
    )
    for program in programs:
        source = registry.source(program.source_key)
        if (
            source.economy_code != "US"
            or program.event_family not in source.event_families
        ):
            raise ValueError(
                f"U.S. program {program.program_key} differs from its registered source"
            )
    return UnitedStatesBackfillProfileV1(
        registry_id=registry.registry_id,
        programs=programs,
        required_program_keys=tuple(item.program_key for item in programs),
        limitations=(
            "Archive start dates describe first-party routes, not empirical coverage claims.",
            "Every retained coverage count must be independently derived from content-addressed raw artifacts.",
            "Unsupported monthly event consensus remains explicit and must not be filled from proprietary sources.",
        ),
    )


def build_federal_reserve_g17_archive_request(
    registry: OfficialSourceRegistryV1,
    release_date: str,
) -> OfficialSourceRequestV1:
    """Plan one exact historical G.17 plain-text release request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("G.17 request requires a v1 source registry")
    parsed = date.fromisoformat(_iso_date(release_date, "release_date"))
    source = registry.source("us.frb.industrial-production")
    if OfficialSourceFormat.TEXT not in source.formats:
        raise ValueError("G.17 source does not declare plain-text releases")
    compact = parsed.strftime("%Y%m%d")
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=(
            "https://www.federalreserve.gov/releases/g17/" f"{compact}/g17.txt"
        ),
        source_format=OfficialSourceFormat.TEXT,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=parsed.isoformat(),
        window_end=parsed.isoformat(),
    )


def _g17_number(value: str, locator: str) -> tuple[str, float]:
    lexical = value.strip()
    if lexical.startswith("-."):
        normalized = "-0" + lexical[1:]
    elif lexical.startswith("+."):
        normalized = "+0" + lexical[1:]
    elif lexical.startswith("."):
        normalized = "0" + lexical
    else:
        normalized = lexical
    try:
        result = float(normalized)
    except ValueError as exc:
        raise ValueError(f"G.17 numeric value is invalid at {locator}") from exc
    if not math.isfinite(result):
        raise ValueError(f"G.17 numeric value is non-finite at {locator}")
    return lexical, result


def _month_period(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _previous_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def parse_federal_reserve_g17_release(
    snapshot: OfficialRawSnapshotV1,
) -> UnitedStatesReleaseTripletV1:
    """Parse the headline total-IP triplet from a retained G.17 release."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("G.17 parser requires a v1 official snapshot")
    if (
        snapshot.request.source_key != "us.frb.industrial-production"
        or snapshot.request.source_format is not OfficialSourceFormat.TEXT
    ):
        raise ValueError("G.17 parser received a different official source")
    try:
        text = snapshot.content.decode("utf-8-sig")
        source_encoding = "utf-8"
    except UnicodeDecodeError:
        try:
            text = snapshot.content.decode("windows-1252")
            source_encoding = "windows-1252"
        except UnicodeDecodeError as exc:
            raise ValueError(
                "G.17 release is neither UTF-8 nor Windows-1252 text"
            ) from exc
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    release_match = re.search(
        r"For release at\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})\s+"
        r"(?P<meridiem>[ap])\.m\.\s+\((?P<zone>EST|EDT|AM|PM)\)\s+"
        r"(?P<date>[A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
        normalized,
    )
    if release_match is None:
        raise ValueError("G.17 release-time header is missing or changed")
    release_date_parts = re.fullmatch(
        r"(?P<month>[A-Z][a-z]+)\s+(?P<day>\d{1,2}),\s+(?P<year>\d{4})",
        release_match.group("date"),
    )
    if release_date_parts is None:
        raise ValueError("G.17 release date is invalid")
    month_number = _MONTHS.get(release_date_parts.group("month").lower())
    if month_number is None:
        raise ValueError("G.17 release date has an unknown month")
    release_date = date(
        int(release_date_parts.group("year")),
        month_number,
        int(release_date_parts.group("day")),
    )
    hour = int(release_match.group("hour")) % 12
    if release_match.group("meridiem") == "p":
        hour += 12
    lexical = f"{release_date.isoformat()}T{hour:02d}:{release_match.group('minute')}:00"
    timestamp = normalize_official_source_timestamp(
        lexical,
        "America/New_York",
        EconomicTimePrecision.EXACT_MINUTE,
    )
    reported_zone = release_match.group("zone")
    expected_abbreviation = (
        datetime.fromtimestamp(
            timestamp.utc_ns / 1_000_000_000, tz=timezone.utc
        )
        .astimezone(ZoneInfo("America/New_York"))
        .tzname()
    )
    if (
        reported_zone in {"EST", "EDT"}
        and reported_zone != expected_abbreviation
    ):
        raise ValueError(
            "G.17 release timezone abbreviation conflicts with date"
        )

    lines = normalized.splitlines()
    summary_index = next(
        (
            index
            for index, line in enumerate(lines)
            if "industrial production and capacity utilization:  summary"
            in line.lower()
        ),
        None,
    )
    if summary_index is None:
        raise ValueError("G.17 summary table is missing")
    table_lines = lines[summary_index : summary_index + 80]
    month_offset = next(
        (
            index
            for index, line in enumerate(table_lines)
            if "industrial production" in line.lower() and line.count("|") >= 2
        ),
        None,
    )
    if month_offset is None:
        raise ValueError("G.17 industrial-production month header is missing")
    month_line = table_lines[month_offset]
    first_month_segment = month_line.split("|")[1]
    month_names = _G17_MONTH_RE.findall(first_month_segment)
    if len(month_names) < 2:
        raise ValueError("G.17 month header has fewer than two periods")
    try:
        current_month = _MONTHS[month_names[-1].rstrip(".").lower()]
    except KeyError as exc:
        raise ValueError("G.17 month header contains an unknown month") from exc
    reference_year = release_date.year
    if current_month > release_date.month:
        reference_year -= 1
    previous_year, previous_month = _previous_month(
        reference_year, current_month
    )

    total_offset = next(
        (
            index
            for index, line in enumerate(
                table_lines[month_offset + 1 :], start=month_offset + 1
            )
            if re.match(r"\s*Total index\s+\|", line)
        ),
        None,
    )
    if total_offset is None:
        raise ValueError("G.17 total-index row is missing")
    total_line = table_lines[total_offset]
    total_segments = total_line.split("|")
    if len(total_segments) < 3:
        raise ValueError("G.17 total-index row shape changed")
    change_tokens = _NUMBER_RE.findall(total_segments[2])
    if len(change_tokens) < 2:
        raise ValueError("G.17 total-index changes are incomplete")
    previous_offset = next(
        (
            index
            for index, line in enumerate(
                table_lines[total_offset + 1 :], start=total_offset + 1
            )
            if "Previous estimates" in line
        ),
        None,
    )
    if previous_offset is None:
        raise ValueError("G.17 previous-estimates row is missing")
    previous_line = table_lines[previous_offset]
    previous_segments = previous_line.split("|")
    if len(previous_segments) < 3:
        raise ValueError("G.17 previous-estimates row shape changed")
    previous_tokens = _NUMBER_RE.findall(previous_segments[2])
    if not previous_tokens:
        raise ValueError("G.17 previous-estimate changes are missing")
    actual_lexical, actual = _g17_number(
        change_tokens[-1],
        f"line:{summary_index + total_offset + 1}:change:last",
    )
    revised_lexical, revised = _g17_number(
        change_tokens[-2],
        f"line:{summary_index + total_offset + 1}:change:previous",
    )
    previous_lexical, previous = _g17_number(
        previous_tokens[-1],
        f"line:{summary_index + previous_offset + 1}:change:previous",
    )
    release_line_number = normalized[: release_match.start()].count("\n") + 1
    total_line_number = summary_index + total_offset + 1
    previous_line_number = summary_index + previous_offset + 1
    parser_limitations: tuple[str, ...] = (
        (
            "This parser qualifies the total industrial-production monthly "
            "percent change only."
        ),
        (
            "Capacity utilization and component rows require separate series "
            "identities."
        ),
        (
            f"The official header reports {reported_zone}; timezone "
            "normalization uses America/New_York."
        ),
    )
    if source_encoding != "utf-8":
        parser_limitations += (
            f"The retained source text decodes as {source_encoding}.",
        )
    if reported_zone not in {"EST", "EDT"}:
        parser_limitations += (
            (
                "The parenthetical header label is not a timezone "
                f"abbreviation; the release date resolves to "
                f"{expected_abbreviation}."
            ),
        )
    return UnitedStatesReleaseTripletV1(
        program_key="us.frb.industrial-production",
        logical_event_key=(
            "us.frb.industrial-production.total-index.mom."
            f"{_month_period(reference_year, current_month)}"
        ),
        event_family=EconomicEventFamily.INDUSTRIAL_PRODUCTION,
        reference_period=_month_period(reference_year, current_month),
        previous_reference_period=_month_period(previous_year, previous_month),
        scheduled_lexical=lexical,
        released_lexical=lexical,
        source_timezone="America/New_York",
        scheduled_for_ns=timestamp.utc_ns,
        released_at_ns=timestamp.utc_ns,
        actual_value=actual,
        actual_lexical=actual_lexical,
        previous_as_known_value=previous,
        previous_as_known_lexical=previous_lexical,
        revised_previous_value=revised,
        revised_previous_lexical=revised_lexical,
        unit="percent",
        transformation="month-over-month-percent-change",
        seasonality="seasonally-adjusted",
        snapshot_id=snapshot.snapshot_id,
        content_sha256=snapshot.content_sha256,
        source_locator=snapshot.resolved_uri,
        release_time_locator=f"line:{release_line_number}:release-header",
        actual_locator=f"line:{total_line_number}:total-index:last-change",
        previous_locator=(
            f"line:{previous_line_number}:previous-estimates:last-change"
        ),
        revision_locator=(
            f"line:{total_line_number}:total-index:penultimate-change"
        ),
        limitations=parser_limitations,
    )


def economic_calendar_release_from_g17_triplet(
    triplet: UnitedStatesReleaseTripletV1,
    snapshot: OfficialRawSnapshotV1,
) -> EconomicCalendarReleaseV1:
    """Project a verified G.17 triplet into the generic economic calendar."""
    if not isinstance(triplet, UnitedStatesReleaseTripletV1):
        raise TypeError("G.17 projection requires a v1 release triplet")
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("G.17 projection requires a v1 official snapshot")
    if (
        triplet.snapshot_id != snapshot.snapshot_id
        or triplet.content_sha256 != snapshot.content_sha256
    ):
        raise ValueError("G.17 triplet differs from its raw snapshot")
    _, period_year, period_month = _year_month(
        triplet.reference_period, "reference_period"
    )
    last_day = calendar.monthrange(period_year, period_month)[1]
    reference_end = datetime(
        period_year,
        period_month,
        last_day,
        23,
        59,
        59,
        tzinfo=timezone.utc,
    )
    source = MarketContextSourceV1(
        name="Federal Reserve G.17 archived statistical release",
        source_version=triplet.reference_period,
        retrieved_at_ns=snapshot.retrieved_at_ns,
        content_sha256=snapshot.content_sha256,
        adapter_name="federal-reserve-g17-release",
        adapter_version="1",
        license_name="United States federal government publication",
        redistribution_allowed=True,
        redistribution_constraints=(
            "Attribute the Board of Governors of the Federal Reserve System.",
        ),
        limitations=triplet.limitations,
        source_uri=snapshot.resolved_uri,
        metadata={
            "snapshot_id": snapshot.snapshot_id,
            "request_id": snapshot.request.request_id,
            "triplet_id": triplet.triplet_id,
            "previous_as_known_value": triplet.previous_as_known_value,
            "revised_previous_value": triplet.revised_previous_value,
        },
    )
    scheduled_with_offset = (
        datetime.fromtimestamp(
            triplet.scheduled_for_ns / 1_000_000_000,
            tz=timezone.utc,
        )
        .astimezone(ZoneInfo(triplet.source_timezone))
        .isoformat()
    )
    released_with_offset = (
        datetime.fromtimestamp(
            triplet.released_at_ns / 1_000_000_000,
            tz=timezone.utc,
        )
        .astimezone(ZoneInfo(triplet.source_timezone))
        .isoformat()
    )
    release_compact = triplet.released_lexical[:10].replace("-", "")
    return EconomicCalendarReleaseV1(
        logical_event_key=triplet.logical_event_key,
        series_key="us.frb.g17.total-ip.sa.mom",
        series_version="g17-1997-base",
        comparability_bridge_id=None,
        economy="United States",
        economy_code="US",
        currency="USD",
        institution="Board of Governors of the Federal Reserve System",
        event_family=EconomicEventFamily.INDUSTRIAL_PRODUCTION,
        indicator_id="industrial-production-total-index",
        source_series_id="g17-total-index",
        source_table_id="g17-summary-total-index-percent-change",
        source_release_id=f"g17.{release_compact}",
        source_request_id=snapshot.request.request_id,
        title="Industrial Production: Total Index",
        reference_period=triplet.reference_period,
        reference_period_end_ns=int(reference_end.timestamp() * 1_000_000_000),
        frequency="monthly",
        seasonality=triplet.seasonality,
        unit=triplet.unit,
        scale=1.0,
        base=None,
        stage=EconomicReleaseStage.INITIAL,
        status=EconomicReleaseStatus.RELEASED,
        scheduled_for_ns=triplet.scheduled_for_ns,
        scheduled_lexical=scheduled_with_offset,
        released_at_ns=triplet.released_at_ns,
        released_lexical=released_with_offset,
        first_observed_at_ns=snapshot.retrieved_at_ns,
        available_at_ns=triplet.released_at_ns,
        source_timezone=triplet.source_timezone,
        timezone_evidence=triplet.release_time_locator,
        time_precision=EconomicTimePrecision.EXACT_MINUTE,
        precision=MarketContextPrecision.EXACT,
        market_context_kind=MarketContextKind.MACRO_RELEASE,
        source=source,
        affected_currencies=("USD",),
        affected_symbols=("EURUSD", "USDJPY"),
        limitations=triplet.limitations,
        actual_value=triplet.actual_value,
        actual_lexical=triplet.actual_lexical,
        content_sha256=triplet.content_sha256,
    )


G17_2002_12_17_URI = (
    "https://www.federalreserve.gov/releases/g17/20021217/g17.txt"
)
G17_2002_12_17_SHA256 = (
    "5f4a2be1e9dfd9de4656062c86f5693680ab4b8e779677558488874296d9c5c9"
)
G17_2002_12_17_NORMALIZED_SHA256 = (
    "bf77819b7c3ebac3588c30eb6372ab906e69f3caa46c5ea7d75905ef894ca885"
)


def packaged_federal_reserve_g17_2002_path() -> Path:
    """Return the installed base64 envelope for the exact G.17 release."""
    return (
        Path(__file__).resolve().parent / "assets" / "us_g17_20021217.txt.b64"
    )


def load_packaged_federal_reserve_g17_2002() -> bytes:
    """Decode and verify the retained first-party G.17 release bytes."""
    path = packaged_federal_reserve_g17_2002_path()
    if path.stat().st_size > 256 * 1024:
        raise ValueError("packaged G.17 base64 envelope exceeds the size bound")
    try:
        encoded = b"".join(path.read_bytes().split())
        content = base64.b64decode(encoded, validate=True)
    except (OSError, ValueError) as exc:
        raise ValueError("packaged G.17 base64 envelope is invalid") from exc
    if hashlib.sha256(content).hexdigest() != G17_2002_12_17_SHA256:
        raise ValueError("packaged G.17 raw artifact hash differs")
    return content


def federal_reserve_g17_2002_artifact_lock() -> UnitedStatesArtifactLockV1:
    """Return the pinned real-release expectations for the 2002 G.17 fixture."""
    return UnitedStatesArtifactLockV1(
        program_key="us.frb.industrial-production",
        artifact_uri=G17_2002_12_17_URI,
        content_sha256=G17_2002_12_17_SHA256,
        expected_reference_period="2002-11",
        expected_actual_value=0.1,
        expected_previous_as_known_value=-0.8,
        expected_revised_previous_value=-0.6,
        expected_release_lexical="2002-12-17T09:15:00",
        normalized_sha256=G17_2002_12_17_NORMALIZED_SHA256,
    )


__all__ = [
    "G17_2002_12_17_NORMALIZED_SHA256",
    "G17_2002_12_17_SHA256",
    "G17_2002_12_17_URI",
    "MAX_US_COVERAGE_ARTIFACTS",
    "MAX_US_PROGRAMS",
    "US_ARTIFACT_LOCK_SCHEMA_VERSION",
    "US_BACKFILL_AUDIT_SCHEMA_VERSION",
    "US_BACKFILL_PROFILE_SCHEMA_VERSION",
    "US_BACKFILL_START_DATE",
    "US_OFFICIAL_PROGRAM_SCHEMA_VERSION",
    "US_PROGRAM_COVERAGE_SCHEMA_VERSION",
    "US_RELEASE_TRIPLET_SCHEMA_VERSION",
    "UnitedStatesArchiveStrategy",
    "UnitedStatesArtifactLockV1",
    "UnitedStatesBackfillAuditV1",
    "UnitedStatesBackfillProfileV1",
    "UnitedStatesCoverageGapReason",
    "UnitedStatesForecastStrategy",
    "UnitedStatesOfficialProgramV1",
    "UnitedStatesProgramCoverageV1",
    "UnitedStatesReleaseTripletV1",
    "audit_united_states_backfill",
    "build_federal_reserve_g17_archive_request",
    "built_in_united_states_backfill_profile",
    "economic_calendar_release_from_g17_triplet",
    "federal_reserve_g17_2002_artifact_lock",
    "load_packaged_federal_reserve_g17_2002",
    "packaged_federal_reserve_g17_2002_path",
    "parse_federal_reserve_g17_release",
]
