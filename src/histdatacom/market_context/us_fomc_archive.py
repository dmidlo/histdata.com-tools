"""Deterministic qualification of the official 2000-present FOMC archive.

The Federal Reserve publishes one current calendar plus annual historical
pages.  Those pages bind policy statements, minutes, and Summary of Economic
Projections (SEP) publications to meetings, conference calls, and notation
votes.  This module keeps that container relationship explicit: a link is
never classified merely because its URL resembles another FOMC document.

The compact packaged assets retain the exact index-page bytes and a manifest
of content-addressed documents.  The larger statement/minutes/SEP corpus is
caller-retained and can be replayed byte for byte with the refresh command.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from html.parser import HTMLParser
from io import BytesIO
from itertools import pairwise
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, cast
from urllib.parse import urldefrag, urljoin, urlparse

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.monetary_policy import (
    MonetaryPolicyActionTiming,
    MonetaryPolicyDecisionDirection,
    MonetaryPolicySettingKind,
    MonetaryPolicySettingV1,
)
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.market_context.us_backfill import (
    US_BACKFILL_START_DATE,
    UnitedStatesBackfillProfileV1,
    UnitedStatesCoverageGapReason,
    UnitedStatesProgramCoverageV1,
)
from histdatacom.runtime_contracts import JSONValue

FOMC_SOURCE_KEY: Final = "us.frb.fomc"
FOMC_PARSER_ID: Final = "official.federal-reserve-fomc.v1"
FOMC_DECISION_PROGRAM_KEY: Final = "us.fomc.decision"
FOMC_MINUTES_PROGRAM_KEY: Final = "us.fomc.minutes"
FOMC_SEP_PROGRAM_KEY: Final = "us.fomc.sep"
FOMC_CURRENT_CALENDAR_URI: Final = (
    "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
)
FOMC_HISTORICAL_URI_TEMPLATE: Final = (
    "https://www.federalreserve.gov/monetarypolicy/fomchistorical{year}.htm"
)
FOMC_HISTORICAL_INDEX_URI: Final = (
    "https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm"
)
FOMC_FIRST_YEAR: Final = 2000
FOMC_LAST_HISTORICAL_YEAR: Final = 2020
FOMC_LATEST_PACKAGED_DECISION_DATE: Final = "2026-07-29"

_FOMC_NON_RATE_DIRECTIONS: Final[
    Mapping[str, MonetaryPolicyDecisionDirection]
] = MappingProxyType(
    {
        "https://www.federalreserve.gov/newsevents/press/monetary/20070817b.htm": (
            MonetaryPolicyDecisionDirection.GUIDANCE_ONLY
        ),
        "https://www.federalreserve.gov/newsevents/press/monetary/20080311a.htm": (
            MonetaryPolicyDecisionDirection.OPERATIONAL
        ),
        "https://www.federalreserve.gov/newsevents/press/monetary/20100509a.htm": (
            MonetaryPolicyDecisionDirection.OPERATIONAL
        ),
    }
)

FOMC_ARTIFACT_SCHEMA_VERSION: Final = "histdatacom.us-fomc-artifact.v1"
FOMC_INDEX_ENTRY_SCHEMA_VERSION: Final = "histdatacom.us-fomc-index-entry.v1"
FOMC_RELEASE_INDEX_SCHEMA_VERSION: Final = (
    "histdatacom.us-fomc-release-index.v1"
)
FOMC_DECISION_SCHEMA_VERSION: Final = "histdatacom.us-fomc-decision.v1"
FOMC_DOCUMENT_SCHEMA_VERSION: Final = "histdatacom.us-fomc-document.v1"
FOMC_MANIFEST_SCHEMA_VERSION: Final = "histdatacom.us-fomc-archive-manifest.v1"

MAX_FOMC_INDEX_PAGES: Final = 32
MAX_FOMC_INDEX_BYTES: Final = 2_000_000
MAX_FOMC_ARTIFACT_BYTES: Final = 20_000_000
MAX_FOMC_DOCUMENTS: Final = 1_024
MAX_FOMC_TOTAL_BYTES: Final = 512_000_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_DATE_IN_URI_RE = re.compile(r"(?<!\d)((?:19|20)\d{6})(?!\d)")
_MINUTES_RELEASE_RE = re.compile(
    r"\bReleased\s+([A-Z][a-z]+\s+\d{1,2},\s+20\d{2})\b",
    re.IGNORECASE,
)
_EXACT_TIME_RE = re.compile(
    r"\bFor\s+release\s+at\s+(\d{1,2}):(\d{2})\s*"
    r"([ap])\.?m\.?(?:\s+|\s*\()?(E[DS]T)\)?",
    re.IGNORECASE,
)
_NUMBER_TOKEN = r"(?:zero|\d+(?:\.\d+)?(?:\s*[- ]\s*\d+/\d+)?|\d+/\d+)"
_RANGE_PATTERNS = (
    re.compile(
        r"(?:raise|lower|keep|maintain|leave|establish)(?:s|ed)?"
        r"[^.]{0,180}?target\s+range\s+for\s+the\s+federal\s+funds\s+rate"
        r"[^.]{0,120}?(?:at|to|of)\s+(?P<lower>"
        + _NUMBER_TOKEN
        + r")\s+to\s+(?P<upper>"
        + _NUMBER_TOKEN
        + r")\s+percent",
        re.IGNORECASE,
    ),
    re.compile(
        r"target\s+range\s+for\s+the\s+federal\s+funds\s+rate"
        r"[^.]{0,120}?(?:at|to|of)\s+(?P<lower>"
        + _NUMBER_TOKEN
        + r")\s+to\s+(?P<upper>"
        + _NUMBER_TOKEN
        + r")\s+percent",
        re.IGNORECASE,
    ),
    re.compile(
        r"federal\s+funds\s+rate\s+in\s+a\s+target\s+range\s+of\s+"
        r"(?P<lower>"
        + _NUMBER_TOKEN
        + r")\s+to\s+(?P<upper>"
        + _NUMBER_TOKEN
        + r")\s+percent",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?P<lower>"
        + _NUMBER_TOKEN
        + r")\s+to\s+(?P<upper>"
        + _NUMBER_TOKEN
        + r")\s+percent\s+target\s+range\s+for\s+the\s+"
        r"federal\s+funds\s+rate",
        re.IGNORECASE,
    ),
)
_SCALAR_PATTERNS = (
    re.compile(
        r"(?:raise|lower|keep|maintain)(?:s|ed)?[^.]{0,180}?"
        r"target(?:\s+level)?\s+for\s+the\s+federal\s+funds\s+rate"
        r"[^.]{0,120}?(?:at|to|of)\s+(?P<value>"
        + _NUMBER_TOKEN
        + r")\s+percent",
        re.IGNORECASE,
    ),
    re.compile(
        r"target\s+federal\s+funds\s+rate[^.]{0,120}?"
        r"(?:at|to|of)\s+(?P<value>" + _NUMBER_TOKEN + r")\s+percent",
        re.IGNORECASE,
    ),
    re.compile(
        r"target\s+rate\s+(?:at|to|of)\s+(?P<value>"
        + _NUMBER_TOKEN
        + r")\s+percent",
        re.IGNORECASE,
    ),
)
_ACTION_DELTA_RE = re.compile(
    r"\b(?P<verb>raise|lower)(?:s|ed)?\b[^.]{0,160}?\bby\s+"
    r"(?P<amount>"
    + _NUMBER_TOKEN
    + r")\s+(?P<unit>basis\s+points?|percentage\s+points?)",
    re.IGNORECASE,
)


class FomcArtifactRole(str, Enum):
    """Role of one official byte artifact in the FOMC corpus."""

    INDEX = "index"
    STATEMENT = "statement"
    MINUTES = "minutes"
    PROJECTIONS = "projections"


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


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    return result


def _https_uri(value: object, name: str) -> str:
    result = _required_text(value, name)
    parsed = urlparse(result)
    if parsed.scheme != "https" or not parsed.hostname or parsed.fragment:
        raise ValueError(f"{name} must be an HTTPS URI without a fragment")
    return result


def _federal_reserve_uri(value: object, name: str) -> str:
    result = _https_uri(value, name)
    if urlparse(result).hostname != "www.federalreserve.gov":
        raise ValueError(f"{name} must use the official Federal Reserve host")
    return result


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name).lower()
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return result


def _bounded_count(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside its bound")
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


def _canonical_uri(value: str) -> str:
    uri, _ = urldefrag(urljoin("https://www.federalreserve.gov", value))
    if uri.startswith("http://www.federalreserve.gov/"):
        uri = "https://" + uri.removeprefix("http://")
    return _federal_reserve_uri(uri, "artifact_uri")


def _texts(values: Sequence[object], name: str) -> tuple[str, ...]:
    result = tuple(sorted({_required_text(item, name) for item in values}))
    if not result:
        raise ValueError(f"{name} must not be empty")
    return result


@dataclass(frozen=True, slots=True)
class FederalReserveFomcArtifactV1:
    """One immutable official index or publication artifact."""

    role: FomcArtifactRole
    source_uri: str
    content_sha256: str
    content_length: int
    media_type: str
    artifact_id: str = ""
    schema_version: str = FOMC_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FOMC_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported FOMC artifact schema")
        role = FomcArtifactRole(self.role)
        object.__setattr__(self, "role", role)
        object.__setattr__(
            self,
            "source_uri",
            _federal_reserve_uri(self.source_uri, "source_uri"),
        )
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        maximum = (
            MAX_FOMC_INDEX_BYTES
            if role is FomcArtifactRole.INDEX
            else MAX_FOMC_ARTIFACT_BYTES
        )
        _bounded_count(self.content_length, "content_length", maximum)
        media_type = _required_text(self.media_type, "media_type").lower()
        if media_type not in {"text/html", "application/pdf"}:
            raise ValueError("unsupported FOMC artifact media type")
        if (
            role in {FomcArtifactRole.INDEX, FomcArtifactRole.STATEMENT}
            and media_type != "text/html"
        ):
            raise ValueError("FOMC index and statement artifacts must be HTML")
        object.__setattr__(self, "media_type", media_type)
        expected = _stable_id("fomc-artifact", self.identity_payload())
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("FOMC artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role.value,
            "source_uri": self.source_uri,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "media_type": self.media_type,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FederalReserveFomcArtifactV1:
        return cls(
            role=FomcArtifactRole(str(data.get("role", ""))),
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            media_type=str(data.get("media_type", "")),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveFomcIndexEntryV1:
    """One meeting/archive container and its explicitly bound documents."""

    meeting_key: str
    meeting_label: str
    meeting_end_date: str
    action_timing: MonetaryPolicyActionTiming
    index_uri: str
    statement_uri: str | None
    minutes_uri: str | None
    minutes_release_date: str | None
    sep_uri: str | None
    limitations: tuple[str, ...]
    entry_id: str = ""
    schema_version: str = FOMC_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FOMC_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported FOMC index-entry schema")
        object.__setattr__(
            self, "meeting_key", _key(self.meeting_key, "meeting_key")
        )
        object.__setattr__(
            self,
            "meeting_label",
            _required_text(self.meeting_label, "meeting_label"),
        )
        object.__setattr__(
            self,
            "meeting_end_date",
            _iso_date(self.meeting_end_date, "meeting_end_date"),
        )
        object.__setattr__(
            self,
            "action_timing",
            MonetaryPolicyActionTiming(self.action_timing),
        )
        object.__setattr__(
            self,
            "index_uri",
            _federal_reserve_uri(self.index_uri, "index_uri"),
        )
        for name in ("statement_uri", "minutes_uri", "sep_uri"):
            value = _optional_text(getattr(self, name))
            object.__setattr__(
                self,
                name,
                None if value is None else _federal_reserve_uri(value, name),
            )
        release_date = _optional_text(self.minutes_release_date)
        if release_date is not None:
            release_date = _iso_date(release_date, "minutes_release_date")
        if self.minutes_uri is None and release_date is not None:
            raise ValueError("FOMC minutes date lacks an artifact")
        object.__setattr__(self, "minutes_release_date", release_date)
        object.__setattr__(
            self, "limitations", _texts(self.limitations, "limitations")
        )
        expected = _stable_id("fomc-index-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("FOMC index-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "meeting_key": self.meeting_key,
            "meeting_label": self.meeting_label,
            "meeting_end_date": self.meeting_end_date,
            "action_timing": self.action_timing.value,
            "index_uri": self.index_uri,
            "statement_uri": self.statement_uri,
            "minutes_uri": self.minutes_uri,
            "minutes_release_date": self.minutes_release_date,
            "sep_uri": self.sep_uri,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FederalReserveFomcIndexEntryV1:
        return cls(
            meeting_key=str(data.get("meeting_key", "")),
            meeting_label=str(data.get("meeting_label", "")),
            meeting_end_date=str(data.get("meeting_end_date", "")),
            action_timing=MonetaryPolicyActionTiming(
                str(data.get("action_timing", ""))
            ),
            index_uri=str(data.get("index_uri", "")),
            statement_uri=_optional_text(data.get("statement_uri")),
            minutes_uri=_optional_text(data.get("minutes_uri")),
            minutes_release_date=_optional_text(
                data.get("minutes_release_date")
            ),
            sep_uri=_optional_text(data.get("sep_uri")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveFomcReleaseIndexV1:
    """Hash-bound union of annual historical pages and the current calendar."""

    as_of_date: str
    index_artifacts: tuple[FederalReserveFomcArtifactV1, ...]
    entries: tuple[FederalReserveFomcIndexEntryV1, ...]
    index_id: str = ""
    schema_version: str = FOMC_RELEASE_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FOMC_RELEASE_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported FOMC release-index schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        artifacts = tuple(
            sorted(self.index_artifacts, key=lambda item: item.source_uri)
        )
        if len(artifacts) != 22 or any(
            item.role is not FomcArtifactRole.INDEX for item in artifacts
        ):
            raise ValueError(
                "FOMC release index requires 22 official index pages"
            )
        if len({item.source_uri for item in artifacts}) != len(artifacts):
            raise ValueError("FOMC release index repeats an index URI")
        expected_artifact_uris = {
            FOMC_HISTORICAL_URI_TEMPLATE.format(year=year)
            for year in range(FOMC_FIRST_YEAR, FOMC_LAST_HISTORICAL_YEAR + 1)
        } | {FOMC_CURRENT_CALENDAR_URI}
        if {item.source_uri for item in artifacts} != expected_artifact_uris:
            raise ValueError("FOMC release-index artifact inventory differs")
        entries = tuple(
            sorted(
                self.entries,
                key=lambda item: (item.meeting_end_date, item.meeting_key),
            )
        )
        if not entries or len(entries) > MAX_FOMC_DOCUMENTS:
            raise ValueError("FOMC index-entry count is outside bounds")
        if len({item.meeting_key for item in entries}) != len(entries):
            raise ValueError("FOMC release index repeats a meeting key")
        artifact_uris = {item.source_uri for item in artifacts}
        if any(item.index_uri not in artifact_uris for item in entries):
            raise ValueError("FOMC entry names an unretained index page")
        if any(
            item.minutes_uri is not None and item.minutes_release_date is None
            for item in entries
        ):
            raise ValueError("FOMC release index has unresolved minutes dates")
        if any(item.meeting_end_date > as_of for item in entries):
            raise ValueError("FOMC entry follows the as-of boundary")
        if any(
            item.minutes_release_date is not None
            and item.minutes_release_date > as_of
            for item in entries
        ):
            raise ValueError(
                "FOMC minutes publication follows the as-of boundary"
            )
        if any(
            item.statement_uri is not None
            and _date_from_uri(item.statement_uri) > as_of
            for item in entries
        ):
            raise ValueError("FOMC statement follows the as-of boundary")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "index_artifacts", artifacts)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id("fomc-release-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("FOMC release-index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_meeting_key(self) -> Mapping[str, FederalReserveFomcIndexEntryV1]:
        return MappingProxyType(
            {item.meeting_key: item for item in self.entries}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "index_artifacts": [
                item.to_dict() for item in self.index_artifacts
            ],
            "entries": [item.to_dict() for item in self.entries],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FederalReserveFomcReleaseIndexV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            index_artifacts=tuple(
                FederalReserveFomcArtifactV1.from_dict(
                    _mapping(item, "index artifact")
                )
                for item in _sequence(
                    data.get("index_artifacts"), "index_artifacts"
                )
            ),
            entries=tuple(
                FederalReserveFomcIndexEntryV1.from_dict(
                    _mapping(item, "index entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveFomcDecisionV1:
    """One statement-backed policy setting and its prior known setting."""

    meeting_key: str
    release_date: str
    release_time_local: str | None
    time_zone_abbreviation: str | None
    action_timing: MonetaryPolicyActionTiming
    previous_setting: MonetaryPolicySettingV1
    new_setting: MonetaryPolicySettingV1
    direction: MonetaryPolicyDecisionDirection
    statement: FederalReserveFomcArtifactV1
    expectation_unavailable_reason: str
    limitations: tuple[str, ...]
    decision_id: str = ""
    schema_version: str = FOMC_DECISION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FOMC_DECISION_SCHEMA_VERSION:
            raise ValueError("unsupported FOMC decision schema")
        object.__setattr__(
            self, "meeting_key", _key(self.meeting_key, "meeting_key")
        )
        object.__setattr__(
            self, "release_date", _iso_date(self.release_date, "release_date")
        )
        local_time = _optional_text(self.release_time_local)
        zone = _optional_text(self.time_zone_abbreviation)
        if (local_time is None) != (zone is None):
            raise ValueError("FOMC exact time requires both clock and zone")
        if (
            local_time is not None
            and re.fullmatch(r"\d{2}:\d{2}", local_time) is None
        ):
            raise ValueError("FOMC release_time_local must be HH:MM")
        if zone is not None and zone not in {"EST", "EDT"}:
            raise ValueError("unsupported FOMC time-zone abbreviation")
        object.__setattr__(self, "release_time_local", local_time)
        object.__setattr__(self, "time_zone_abbreviation", zone)
        object.__setattr__(
            self,
            "action_timing",
            MonetaryPolicyActionTiming(self.action_timing),
        )
        if not isinstance(
            self.previous_setting, MonetaryPolicySettingV1
        ) or not isinstance(self.new_setting, MonetaryPolicySettingV1):
            raise TypeError("FOMC decision requires typed policy settings")
        direction = MonetaryPolicyDecisionDirection(self.direction)
        equal = (
            self.previous_setting.semantic_id == self.new_setting.semantic_id
        )
        unchanged_directions = {
            MonetaryPolicyDecisionDirection.HOLD,
            MonetaryPolicyDecisionDirection.GUIDANCE_ONLY,
            MonetaryPolicyDecisionDirection.OPERATIONAL,
        }
        if direction in unchanged_directions and not equal:
            raise ValueError("FOMC non-rate action changes the policy setting")
        if (
            direction
            in {
                MonetaryPolicyDecisionDirection.EASE,
                MonetaryPolicyDecisionDirection.TIGHTEN,
            }
            and equal
        ):
            raise ValueError("FOMC rate action leaves the setting unchanged")
        if direction not in unchanged_directions | {
            MonetaryPolicyDecisionDirection.EASE,
            MonetaryPolicyDecisionDirection.TIGHTEN,
        }:
            raise ValueError("unsupported FOMC decision direction")
        object.__setattr__(self, "direction", direction)
        if self.statement.role is not FomcArtifactRole.STATEMENT:
            raise ValueError("FOMC decision requires a statement artifact")
        object.__setattr__(
            self,
            "expectation_unavailable_reason",
            _required_text(
                self.expectation_unavailable_reason,
                "expectation_unavailable_reason",
            ),
        )
        object.__setattr__(
            self, "limitations", _texts(self.limitations, "limitations")
        )
        expected = _stable_id("fomc-decision", self.identity_payload())
        if self.decision_id and self.decision_id != expected:
            raise ValueError("FOMC decision identity differs")
        object.__setattr__(self, "decision_id", expected)

    @property
    def exact_minute(self) -> bool:
        return self.release_time_local is not None

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "meeting_key": self.meeting_key,
            "release_date": self.release_date,
            "release_time_local": self.release_time_local,
            "time_zone_abbreviation": self.time_zone_abbreviation,
            "action_timing": self.action_timing.value,
            "previous_setting": self.previous_setting.to_dict(),
            "new_setting": self.new_setting.to_dict(),
            "direction": self.direction.value,
            "statement": self.statement.to_dict(),
            "expectation_unavailable_reason": self.expectation_unavailable_reason,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "decision_id": self.decision_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FederalReserveFomcDecisionV1:
        return cls(
            meeting_key=str(data.get("meeting_key", "")),
            release_date=str(data.get("release_date", "")),
            release_time_local=_optional_text(data.get("release_time_local")),
            time_zone_abbreviation=_optional_text(
                data.get("time_zone_abbreviation")
            ),
            action_timing=MonetaryPolicyActionTiming(
                str(data.get("action_timing", ""))
            ),
            previous_setting=MonetaryPolicySettingV1.from_dict(
                _mapping(data.get("previous_setting"), "previous_setting")
            ),
            new_setting=MonetaryPolicySettingV1.from_dict(
                _mapping(data.get("new_setting"), "new_setting")
            ),
            direction=MonetaryPolicyDecisionDirection(
                str(data.get("direction", ""))
            ),
            statement=FederalReserveFomcArtifactV1.from_dict(
                _mapping(data.get("statement"), "statement")
            ),
            expectation_unavailable_reason=str(
                data.get("expectation_unavailable_reason", "")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            decision_id=str(data.get("decision_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveFomcDocumentV1:
    """One independently published minutes or SEP document."""

    role: FomcArtifactRole
    meeting_keys: tuple[str, ...]
    meeting_end_date: str
    release_date: str
    artifact: FederalReserveFomcArtifactV1
    limitations: tuple[str, ...]
    document_id: str = ""
    schema_version: str = FOMC_DOCUMENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FOMC_DOCUMENT_SCHEMA_VERSION:
            raise ValueError("unsupported FOMC document schema")
        role = FomcArtifactRole(self.role)
        if role not in {FomcArtifactRole.MINUTES, FomcArtifactRole.PROJECTIONS}:
            raise ValueError(
                "FOMC document role must be minutes or projections"
            )
        object.__setattr__(self, "role", role)
        keys = tuple(
            sorted({_key(item, "meeting_keys") for item in self.meeting_keys})
        )
        if not keys:
            raise ValueError("FOMC document requires a meeting")
        object.__setattr__(self, "meeting_keys", keys)
        end = _iso_date(self.meeting_end_date, "meeting_end_date")
        release = _iso_date(self.release_date, "release_date")
        if release < end:
            raise ValueError("FOMC document release precedes its meeting")
        object.__setattr__(self, "meeting_end_date", end)
        object.__setattr__(self, "release_date", release)
        if self.artifact.role is not role:
            raise ValueError("FOMC document and artifact roles differ")
        object.__setattr__(
            self, "limitations", _texts(self.limitations, "limitations")
        )
        expected = _stable_id("fomc-document", self.identity_payload())
        if self.document_id and self.document_id != expected:
            raise ValueError("FOMC document identity differs")
        object.__setattr__(self, "document_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role.value,
            "meeting_keys": list(self.meeting_keys),
            "meeting_end_date": self.meeting_end_date,
            "release_date": self.release_date,
            "artifact": self.artifact.to_dict(),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "document_id": self.document_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FederalReserveFomcDocumentV1:
        return cls(
            role=FomcArtifactRole(str(data.get("role", ""))),
            meeting_keys=tuple(
                str(item)
                for item in _sequence(data.get("meeting_keys"), "meeting_keys")
            ),
            meeting_end_date=str(data.get("meeting_end_date", "")),
            release_date=str(data.get("release_date", "")),
            artifact=FederalReserveFomcArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            document_id=str(data.get("document_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveFomcArchiveManifestV1:
    """Complete content-addressed decision, minutes, and SEP qualification."""

    registry_id: str
    profile_id: str
    release_index: FederalReserveFomcReleaseIndexV1
    decisions: tuple[FederalReserveFomcDecisionV1, ...]
    minutes: tuple[FederalReserveFomcDocumentV1, ...]
    projections: tuple[FederalReserveFomcDocumentV1, ...]
    raw_artifact_count: int
    total_content_bytes: int
    exact_minute_decision_count: int
    unscheduled_decision_count: int
    manifest_id: str = ""
    schema_version: str = FOMC_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FOMC_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported FOMC archive-manifest schema")
        if not isinstance(self.release_index, FederalReserveFomcReleaseIndexV1):
            raise TypeError("FOMC manifest requires a v1 release index")
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith(
            "official-source-registry:sha256:"
        ) or not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError(
                "FOMC manifest registry/profile identity is invalid"
            )
        decisions = tuple(
            sorted(
                self.decisions,
                key=lambda item: (item.release_date, item.meeting_key),
            )
        )
        minutes = tuple(
            sorted(
                self.minutes,
                key=lambda item: (item.release_date, item.artifact.source_uri),
            )
        )
        projections = tuple(
            sorted(
                self.projections,
                key=lambda item: (item.release_date, item.artifact.source_uri),
            )
        )
        if not decisions or not minutes or not projections:
            raise ValueError(
                "FOMC manifest requires all three program families"
            )
        if len({item.statement.source_uri for item in decisions}) != len(
            decisions
        ):
            raise ValueError("FOMC decisions repeat a statement artifact")
        documents = (*minutes, *projections)
        if len({item.artifact.source_uri for item in minutes}) != len(minutes):
            raise ValueError("FOMC minutes repeat an artifact URI")
        if len({item.artifact.source_uri for item in projections}) != len(
            projections
        ):
            raise ValueError("FOMC projections repeat an artifact URI")
        if any(
            item.role is not FomcArtifactRole.MINUTES for item in minutes
        ) or any(
            item.role is not FomcArtifactRole.PROJECTIONS
            for item in projections
        ):
            raise ValueError("FOMC document family differs from manifest field")
        index_by_key = self.release_index.by_meeting_key
        expected_statement_uris = {
            item.statement_uri
            for item in self.release_index.entries
            if item.statement_uri is not None
        }
        if {item.statement.source_uri for item in decisions} != (
            expected_statement_uris
        ):
            raise ValueError(
                "FOMC decision inventory differs from release index"
            )
        for decision in decisions:
            entry = index_by_key.get(decision.meeting_key)
            if (
                entry is None
                or entry.statement_uri != decision.statement.source_uri
                or entry.action_timing is not decision.action_timing
                or _date_from_uri(decision.statement.source_uri)
                != decision.release_date
                or decision.release_date > self.release_index.as_of_date
            ):
                raise ValueError("FOMC decision differs from its index entry")
            expected_direction = _direction(
                decision.previous_setting, decision.new_setting
            )
            if decision.direction in {
                MonetaryPolicyDecisionDirection.GUIDANCE_ONLY,
                MonetaryPolicyDecisionDirection.OPERATIONAL,
            }:
                expected_direction = _FOMC_NON_RATE_DIRECTIONS.get(
                    decision.statement.source_uri,
                    expected_direction,
                )
            if decision.direction is not expected_direction:
                raise ValueError(
                    "FOMC decision direction differs from settings"
                )
        if any(
            current.previous_setting.semantic_id
            != previous.new_setting.semantic_id
            for previous, current in pairwise(decisions)
        ):
            raise ValueError("FOMC previous-setting lineage differs")

        expected_minutes: dict[str, list[FederalReserveFomcIndexEntryV1]] = (
            defaultdict(list)
        )
        expected_projections: dict[
            str, list[FederalReserveFomcIndexEntryV1]
        ] = defaultdict(list)
        for entry in self.release_index.entries:
            if entry.minutes_uri is not None:
                expected_minutes[entry.minutes_uri].append(entry)
            if entry.sep_uri is not None:
                expected_projections[entry.sep_uri].append(entry)
        if {item.artifact.source_uri for item in minutes} != set(
            expected_minutes
        ):
            raise ValueError(
                "FOMC minutes inventory differs from release index"
            )
        if {item.artifact.source_uri for item in projections} != set(
            expected_projections
        ):
            raise ValueError("FOMC SEP inventory differs from release index")
        for document in minutes:
            indexed = expected_minutes[document.artifact.source_uri]
            release_dates = {item.minutes_release_date for item in indexed}
            if (
                document.meeting_keys
                != tuple(sorted(item.meeting_key for item in indexed))
                or document.meeting_end_date
                != max(item.meeting_end_date for item in indexed)
                or release_dates != {document.release_date}
                or document.release_date > self.release_index.as_of_date
            ):
                raise ValueError("FOMC minutes differ from their index entries")
        for document in projections:
            indexed = expected_projections[document.artifact.source_uri]
            if len(indexed) != 1:
                raise ValueError(
                    "FOMC SEP artifact binds multiple index entries"
                )
            entry = indexed[0]
            expected_release = (
                entry.minutes_release_date
                if entry.sep_uri == entry.minutes_uri
                else entry.meeting_end_date
            )
            if (
                document.meeting_keys != (entry.meeting_key,)
                or document.meeting_end_date != entry.meeting_end_date
                or document.release_date != expected_release
                or document.release_date > self.release_index.as_of_date
            ):
                raise ValueError("FOMC SEP differs from its index entry")
        artifacts = (
            *self.release_index.index_artifacts,
            *(item.statement for item in decisions),
            *(item.artifact for item in documents),
        )
        raw_by_uri: dict[str, FederalReserveFomcArtifactV1] = {}
        for artifact in artifacts:
            retained = raw_by_uri.setdefault(artifact.source_uri, artifact)
            if (
                artifact.content_sha256,
                artifact.content_length,
                artifact.media_type,
            ) != (
                retained.content_sha256,
                retained.content_length,
                retained.media_type,
            ):
                raise ValueError("shared FOMC raw artifact metadata differs")
        expected_counts = (
            len(raw_by_uri),
            sum(item.content_length for item in raw_by_uri.values()),
            sum(item.exact_minute for item in decisions),
            sum(
                item.action_timing is not MonetaryPolicyActionTiming.SCHEDULED
                for item in decisions
            ),
        )
        actual_counts = (
            self.raw_artifact_count,
            self.total_content_bytes,
            self.exact_minute_decision_count,
            self.unscheduled_decision_count,
        )
        if actual_counts != expected_counts:
            raise ValueError("FOMC manifest summary counts differ")
        if self.total_content_bytes > MAX_FOMC_TOTAL_BYTES:
            raise ValueError("FOMC corpus exceeds its byte bound")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "decisions", decisions)
        object.__setattr__(self, "minutes", minutes)
        object.__setattr__(self, "projections", projections)
        expected = _stable_id("fomc-archive-manifest", self.identity_payload())
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("FOMC archive-manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "decisions": [item.to_dict() for item in self.decisions],
            "minutes": [item.to_dict() for item in self.minutes],
            "projections": [item.to_dict() for item in self.projections],
            "raw_artifact_count": self.raw_artifact_count,
            "total_content_bytes": self.total_content_bytes,
            "exact_minute_decision_count": self.exact_minute_decision_count,
            "unscheduled_decision_count": self.unscheduled_decision_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FederalReserveFomcArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=FederalReserveFomcReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            decisions=tuple(
                FederalReserveFomcDecisionV1.from_dict(
                    _mapping(item, "decision")
                )
                for item in _sequence(data.get("decisions"), "decisions")
            ),
            minutes=tuple(
                FederalReserveFomcDocumentV1.from_dict(
                    _mapping(item, "minutes document")
                )
                for item in _sequence(data.get("minutes"), "minutes")
            ),
            projections=tuple(
                FederalReserveFomcDocumentV1.from_dict(
                    _mapping(item, "projection document")
                )
                for item in _sequence(data.get("projections"), "projections")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            exact_minute_decision_count=cast(
                int, data.get("exact_minute_decision_count")
            ),
            unscheduled_decision_count=cast(
                int, data.get("unscheduled_decision_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> FederalReserveFomcArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("FOMC manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "FOMC manifest"))


@dataclass(frozen=True, slots=True)
class _Panel:
    year: int
    text: str
    links: tuple[tuple[str, str], ...]


class _PanelCollector(HTMLParser):
    def __init__(
        self, *, mode: str, historical_year: int | None = None
    ) -> None:
        super().__init__(convert_charrefs=True)
        self.mode = mode
        self.current_year = historical_year
        self.panels: list[_Panel] = []
        self._active = False
        self._div_depth = 0
        self._text: list[str] = []
        self._links: list[tuple[str, str]] = []
        self._anchor_href: str | None = None
        self._anchor_text: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = {key.lower(): value or "" for key, value in attrs}
        classes = set(values.get("class", "").split())
        target = tag.lower() == "div" and (
            (
                self.mode == "historical"
                and {"panel", "panel-default"} <= classes
            )
            or (self.mode == "current" and "fomc-meeting" in classes)
        )
        if target and not self._active:
            self._active = True
            self._div_depth = 1
            self._text = []
            self._links = []
            return
        if self._active and tag.lower() == "div":
            self._div_depth += 1
        if self._active and tag.lower() == "a":
            self._anchor_href = values.get("href")
            self._anchor_text = []

    def handle_endtag(self, tag: str) -> None:
        if (
            self._active
            and tag.lower() == "a"
            and self._anchor_href is not None
        ):
            self._links.append(
                (
                    " ".join(" ".join(self._anchor_text).split()),
                    self._anchor_href,
                )
            )
            self._anchor_href = None
            self._anchor_text = []
        if self._active and tag.lower() == "div":
            self._div_depth -= 1
            if self._div_depth == 0:
                if self.current_year is None:
                    raise ValueError("current FOMC panel has no year heading")
                self.panels.append(
                    _Panel(
                        year=self.current_year,
                        text=" ".join(" ".join(self._text).split()),
                        links=tuple(self._links),
                    )
                )
                self._active = False

    def handle_data(self, data: str) -> None:
        text = " ".join(data.split())
        if not text:
            return
        if not self._active and self.mode == "current":
            match = re.search(r"\b(20\d{2})\s+FOMC\s+Meetings\b", text)
            if match is not None:
                self.current_year = int(match.group(1))
        if self._active:
            self._text.append(text)
            if self._anchor_href is not None:
                self._anchor_text.append(text)


class _VisibleTextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag.lower() in {"script", "style", "noscript", "svg"}:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if (
            tag.lower() in {"script", "style", "noscript", "svg"}
            and self._skip_depth
        ):
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip_depth:
            value = " ".join(data.split())
            if value:
                self.parts.append(value)


def _visible_html_text(content: bytes) -> str:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = content.decode("windows-1252")
        except UnicodeDecodeError as exc:
            raise ValueError(
                "FOMC HTML is neither UTF-8 nor Windows-1252"
            ) from exc
    collector = _VisibleTextCollector()
    try:
        collector.feed(text)
        collector.close()
    except ValueError as exc:
        raise ValueError("FOMC HTML is malformed") from exc
    return " ".join(collector.parts)


def _artifact(
    snapshot: OfficialRawSnapshotV1, role: FomcArtifactRole
) -> FederalReserveFomcArtifactV1:
    if (
        snapshot.request.source_key != FOMC_SOURCE_KEY
        or snapshot.request.parser_id != FOMC_PARSER_ID
    ):
        raise ValueError("FOMC snapshot has the wrong source/parser identity")
    if snapshot.status_code != 200 or not snapshot.content:
        raise ValueError(
            f"FOMC {role.value} snapshot is not a successful artifact"
        )
    expected_format = (
        OfficialSourceFormat.PDF
        if snapshot.request.uri.lower().endswith(".pdf")
        else OfficialSourceFormat.HTML
    )
    if snapshot.request.source_format is not expected_format:
        raise ValueError("FOMC artifact request format differs from its URI")
    if expected_format is OfficialSourceFormat.PDF:
        if not snapshot.content.startswith(b"%PDF-"):
            raise ValueError("FOMC PDF artifact lacks a PDF signature")
        media_type = "application/pdf"
    else:
        text = _visible_html_text(snapshot.content)
        if "Federal Reserve" not in text and "FOMC" not in text:
            raise ValueError("FOMC HTML artifact lacks official content")
        media_type = "text/html"
    return FederalReserveFomcArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        content_sha256=hashlib.sha256(snapshot.content).hexdigest(),
        content_length=len(snapshot.content),
        media_type=media_type,
    )


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    as_of_date: str,
) -> OfficialSourceRequestV1:
    source = registry.source(FOMC_SOURCE_KEY)
    if source.parser_id != FOMC_PARSER_ID or not {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.PDF,
    } <= set(source.formats):
        raise ValueError("FOMC source registry binding differs")
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=_federal_reserve_uri(uri, "request_uri"),
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=US_BACKFILL_START_DATE,
        window_end=as_of_date,
    )


def build_federal_reserve_fomc_index_requests(
    registry: OfficialSourceRegistryV1, *, as_of_date: str
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan the 21 historical pages plus the current FOMC calendar."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("FOMC requests require a v1 registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    uris = tuple(
        FOMC_HISTORICAL_URI_TEMPLATE.format(year=year)
        for year in range(FOMC_FIRST_YEAR, FOMC_LAST_HISTORICAL_YEAR + 1)
    ) + (FOMC_CURRENT_CALENDAR_URI,)
    return tuple(
        _request(registry, uri, OfficialSourceFormat.HTML, as_of_date=as_of)
        for uri in uris
    )


def _month_number(value: str) -> int:
    names = {
        "jan": 1,
        "january": 1,
        "feb": 2,
        "february": 2,
        "mar": 3,
        "march": 3,
        "apr": 4,
        "april": 4,
        "may": 5,
        "jun": 6,
        "june": 6,
        "jul": 7,
        "july": 7,
        "aug": 8,
        "august": 8,
        "sep": 9,
        "sept": 9,
        "september": 9,
        "oct": 10,
        "october": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }
    try:
        return names[value.strip().lower()]
    except KeyError as exc:
        raise ValueError("unsupported FOMC meeting month") from exc


def _meeting_end_date(year: int, label: str) -> str:
    match = re.match(
        r"\s*([A-Za-z]+)(?:/([A-Za-z]+))?\s+(\d{1,2})(?:-(\d{1,2}))?", label
    )
    if match is None:
        raise ValueError(f"cannot parse FOMC meeting date: {label}")
    month = _month_number(match.group(2) or match.group(1))
    day = int(match.group(4) or match.group(3))
    return date(year, month, day).isoformat()


def _slug(value: str) -> str:
    result = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    if not result:
        raise ValueError("FOMC meeting label has no identity")
    return result


def _date_from_uri(uri: str) -> str:
    matches = _DATE_IN_URI_RE.findall(uri)
    if not matches:
        raise ValueError(f"FOMC artifact URI has no date: {uri}")
    raw = matches[-1]
    return date(int(raw[:4]), int(raw[4:6]), int(raw[6:])).isoformat()


def _released_date(text: str) -> str | None:
    match = _MINUTES_RELEASE_RE.search(text)
    if match is None:
        return None
    for pattern in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return (
                datetime.strptime(match.group(1), pattern)
                .replace(tzinfo=timezone.utc)
                .date()
                .isoformat()
            )
        except ValueError:
            continue
    raise ValueError("FOMC minutes release date is malformed")


def _classify_panel(
    panel: _Panel, index_uri: str, *, current: bool
) -> FederalReserveFomcIndexEntryV1:
    links = [(text, _canonical_uri(href)) for text, href in panel.links if href]
    statement: str | None = None
    if current:
        statement = next(
            (
                uri
                for text, uri in links
                if text == "HTML"
                and re.search(r"/pressreleases/monetary20\d{6}a\.htm$", uri)
            ),
            None,
        )
    else:
        statement = next(
            (uri for text, uri in links if text == "Statement"), None
        )

    minute_candidates = [uri for _, uri in links if "minute" in uri.lower()]
    minute_html = [
        uri for uri in minute_candidates if not uri.lower().endswith(".pdf")
    ]
    minutes_uri: str | None = None
    if minute_html:
        minutes_uri = minute_html[0]
    elif minute_candidates:
        minutes_uri = minute_candidates[0]
    if minutes_uri is not None:
        minutes_uri = _canonical_uri(minutes_uri)

    meeting_end = _meeting_end_date(panel.year, panel.text)
    if current:
        sep_candidates = [
            uri
            for text, uri in links
            if text == "HTML"
            and re.search(r"fomcprojtable?\d{8}\.htm$", uri, re.IGNORECASE)
        ]
        sep_uri = sep_candidates[0] if sep_candidates else None
    else:
        has_sep = any(
            re.search(r"SEPcompilation\.pdf$", uri, re.IGNORECASE)
            for _, uri in links
        )
        if not has_sep:
            sep_uri = None
        elif meeting_end < "2011-04-27":
            if minutes_uri is None:
                raise ValueError(
                    "early FOMC SEP has no contemporaneous minutes artifact"
                )
            sep_uri = minutes_uri
        else:
            compact_date = meeting_end.replace("-", "")
            sep_uri = (
                "https://www.federalreserve.gov/monetarypolicy/files/"
                f"fomcprojtabl{compact_date}.pdf"
            )
    label = panel.text.split(" Beige Book", 1)[0].split(" Greenbook", 1)[0]
    label = re.split(
        r"\s+(?:Statement:|Statement\b|Minutes:|Minutes\b|Agenda\b)",
        label,
        maxsplit=1,
    )[0]
    label = " ".join(label.split())
    lowered = label.lower()
    timing = (
        MonetaryPolicyActionTiming.UNSCHEDULED
        if any(
            token in lowered
            for token in ("conference call", "unscheduled", "notation vote")
        )
        else MonetaryPolicyActionTiming.SCHEDULED
    )
    limitations = [
        "Documents are classified only within their official meeting container."
    ]
    if "cancelled" in lowered:
        limitations.append(
            "The official archive marks this meeting as cancelled."
        )
    if "notation vote" in lowered and statement is None:
        limitations.append(
            "A strategy or notation-vote link is not classified as a policy-rate decision statement."
        )
    release_date = (
        _released_date(panel.text) if minutes_uri is not None else None
    )
    if minutes_uri is not None and release_date is None:
        limitations.append(
            "The minutes release date is resolved from another container for the shared document."
        )
    return FederalReserveFomcIndexEntryV1(
        meeting_key=f"fomc.{meeting_end}.{_slug(label)[:96]}",
        meeting_label=label,
        meeting_end_date=meeting_end,
        action_timing=timing,
        index_uri=index_uri,
        statement_uri=statement,
        minutes_uri=minutes_uri,
        minutes_release_date=release_date,
        sep_uri=sep_uri,
        limitations=tuple(limitations),
    )


def parse_federal_reserve_fomc_release_index(
    index_snapshots: Sequence[OfficialRawSnapshotV1], *, as_of_date: str
) -> FederalReserveFomcReleaseIndexV1:
    """Parse and join the official FOMC historical/current index pages."""
    as_of = _iso_date(as_of_date, "as_of_date")
    snapshots = tuple(index_snapshots)
    if len(snapshots) != 22:
        raise ValueError("FOMC release-index parser requires 22 snapshots")
    expected_uris = {
        FOMC_HISTORICAL_URI_TEMPLATE.format(year=year)
        for year in range(FOMC_FIRST_YEAR, FOMC_LAST_HISTORICAL_YEAR + 1)
    } | {FOMC_CURRENT_CALENDAR_URI}
    if {item.request.uri for item in snapshots} != expected_uris:
        raise ValueError("FOMC release-index snapshot inventory differs")
    artifacts: list[FederalReserveFomcArtifactV1] = []
    entries: list[FederalReserveFomcIndexEntryV1] = []
    for snapshot in sorted(snapshots, key=lambda item: item.request.uri):
        artifacts.append(_artifact(snapshot, FomcArtifactRole.INDEX))
        current = snapshot.request.uri == FOMC_CURRENT_CALENDAR_URI
        year_match = re.search(
            r"historical(20\d{2})\.htm$", snapshot.request.uri
        )
        collector = _PanelCollector(
            mode="current" if current else "historical",
            historical_year=(
                None
                if current
                else int(cast(re.Match[str], year_match).group(1))
            ),
        )
        collector.feed(snapshot.content.decode("utf-8-sig"))
        collector.close()
        if not collector.panels:
            raise ValueError(
                f"FOMC index page has no meeting containers: {snapshot.request.uri}"
            )
        for panel in collector.panels:
            entry = _classify_panel(
                panel, snapshot.request.uri, current=current
            )
            if entry.meeting_end_date <= as_of:
                entries.append(entry)

    # Shared historical minute documents use fragments or "see end" labels.
    # Resolve each missing release date from the regular-meeting container that
    # names the same fragment-free document and carries the authored date.
    dates_by_uri: dict[str, set[str]] = defaultdict(set)
    for entry in entries:
        if (
            entry.minutes_uri is not None
            and entry.minutes_release_date is not None
        ):
            dates_by_uri[entry.minutes_uri].add(entry.minutes_release_date)
    repaired: list[FederalReserveFomcIndexEntryV1] = []
    for entry in entries:
        if entry.minutes_uri is None or entry.minutes_release_date is not None:
            repaired.append(entry)
            continue
        dates = dates_by_uri.get(entry.minutes_uri, set())
        if len(dates) != 1:
            # "See end" conference-call rows have no document link and never
            # enter this branch. A linked shared document must be anchored by
            # exactly one published regular-meeting release date.
            raise ValueError(
                f"cannot resolve shared FOMC minutes release date: {entry.minutes_uri}"
            )
        payload = entry.to_dict()
        payload["minutes_release_date"] = next(iter(dates))
        payload["entry_id"] = ""
        repaired.append(FederalReserveFomcIndexEntryV1.from_dict(payload))
    bounded: list[FederalReserveFomcIndexEntryV1] = []
    for entry in repaired:
        statement_after_boundary = (
            entry.statement_uri is not None
            and _date_from_uri(entry.statement_uri) > as_of
        )
        minutes_after_boundary = (
            entry.minutes_release_date is not None
            and entry.minutes_release_date > as_of
        )
        if not statement_after_boundary and not minutes_after_boundary:
            bounded.append(entry)
            continue
        bounded_payload: dict[str, Any] = dict(entry.to_dict())
        limitations = list(entry.limitations)
        if statement_after_boundary:
            bounded_payload["statement_uri"] = None
            limitations.append(
                "The statement publication follows the requested as-of boundary."
            )
        if minutes_after_boundary:
            previous_minutes_uri = entry.minutes_uri
            bounded_payload["minutes_uri"] = None
            bounded_payload["minutes_release_date"] = None
            if entry.sep_uri == previous_minutes_uri:
                bounded_payload["sep_uri"] = None
            limitations.append(
                "The minutes publication follows the requested as-of boundary."
            )
        bounded_payload["limitations"] = limitations
        bounded_payload["entry_id"] = ""
        bounded.append(
            FederalReserveFomcIndexEntryV1.from_dict(bounded_payload)
        )
    return FederalReserveFomcReleaseIndexV1(
        as_of_date=as_of,
        index_artifacts=tuple(artifacts),
        entries=tuple(bounded),
    )


def build_federal_reserve_fomc_document_requests(
    registry: OfficialSourceRegistryV1,
    release_index: FederalReserveFomcReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan every unique statement, minutes, and SEP document fetch."""
    if not isinstance(release_index, FederalReserveFomcReleaseIndexV1):
        raise TypeError("FOMC document requests require a v1 release index")
    uris = sorted(
        {
            uri
            for entry in release_index.entries
            for uri in (entry.statement_uri, entry.minutes_uri, entry.sep_uri)
            if uri is not None
        }
    )
    return tuple(
        _request(
            registry,
            uri,
            (
                OfficialSourceFormat.PDF
                if uri.lower().endswith(".pdf")
                else OfficialSourceFormat.HTML
            ),
            as_of_date=release_index.as_of_date,
        )
        for uri in uris
    )


def _number(value: str) -> float:
    token = (
        value.strip()
        .lower()
        .replace("‑", "-")
        .replace("–", "-")
        .replace("−", "-")
    )
    if token == "zero":
        return 0.0
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*[- ]\s*(\d+)/(\d+)", token)
    if match is not None:
        denominator = int(match.group(3))
        if denominator == 0:
            raise ValueError("FOMC fraction denominator is zero")
        return float(match.group(1)) + int(match.group(2)) / denominator
    match = re.fullmatch(r"(\d+)/(\d+)", token)
    if match is not None:
        denominator = int(match.group(2))
        if denominator == 0:
            raise ValueError("FOMC fraction denominator is zero")
        return int(match.group(1)) / denominator
    result = float(token)
    if not math.isfinite(result):
        raise ValueError("FOMC rate is not finite")
    return result


def _policy_setting(text: str) -> MonetaryPolicySettingV1:
    normalized = text.replace("‑", "-").replace("–", "-").replace("−", "-")
    for pattern in _RANGE_PATTERNS:
        match = pattern.search(normalized)
        if match is not None:
            lower = _number(match.group("lower"))
            upper = _number(match.group("upper"))
            return MonetaryPolicySettingV1(
                kind=MonetaryPolicySettingKind.TARGET_RANGE,
                unit="percent",
                definition_version="fomc-federal-funds-target.v1",
                source_lexical=match.group(0),
                lower_bound=lower,
                upper_bound=upper,
            )
    for pattern in _SCALAR_PATTERNS:
        match = pattern.search(normalized)
        if match is not None:
            return MonetaryPolicySettingV1(
                kind=MonetaryPolicySettingKind.SCALAR_RATE,
                unit="percent",
                definition_version="fomc-federal-funds-target.v1",
                source_lexical=match.group(0),
                scalar_value=_number(match.group("value")),
            )
    raise ValueError(
        "FOMC statement has no supported federal-funds target setting"
    )


def _first_previous_setting(
    text: str, current: MonetaryPolicySettingV1
) -> MonetaryPolicySettingV1:
    match = _ACTION_DELTA_RE.search(text.replace("‑", "-").replace("–", "-"))
    if (
        match is None
        or current.kind is not MonetaryPolicySettingKind.SCALAR_RATE
    ):
        raise ValueError("first FOMC statement cannot derive its prior setting")
    amount = _number(match.group("amount"))
    if match.group("unit").lower().startswith("basis"):
        amount /= 100.0
    assert current.scalar_value is not None
    previous = (
        current.scalar_value - amount
        if match.group("verb").lower().startswith("raise")
        else current.scalar_value + amount
    )
    return MonetaryPolicySettingV1(
        kind=MonetaryPolicySettingKind.SCALAR_RATE,
        unit="percent",
        definition_version="fomc-federal-funds-target.v1",
        source_lexical=f"Derived from source-authored {match.group(0)}",
        scalar_value=previous,
    )


def _midpoint(setting: MonetaryPolicySettingV1) -> float:
    if setting.kind is MonetaryPolicySettingKind.SCALAR_RATE:
        assert setting.scalar_value is not None
        return float(setting.scalar_value)
    if setting.kind is MonetaryPolicySettingKind.TARGET_RANGE:
        assert (
            setting.lower_bound is not None and setting.upper_bound is not None
        )
        lower = float(setting.lower_bound)
        upper = float(setting.upper_bound)
        return (lower + upper) / 2.0
    raise ValueError("unsupported FOMC target-setting shape")


def _direction(
    previous: MonetaryPolicySettingV1, current: MonetaryPolicySettingV1
) -> MonetaryPolicyDecisionDirection:
    delta = _midpoint(current) - _midpoint(previous)
    if abs(delta) < 1e-12:
        return MonetaryPolicyDecisionDirection.HOLD
    return (
        MonetaryPolicyDecisionDirection.TIGHTEN
        if delta > 0
        else MonetaryPolicyDecisionDirection.EASE
    )


def _carried_setting(
    previous: MonetaryPolicySettingV1,
) -> MonetaryPolicySettingV1:
    payload = previous.to_dict()
    payload["source_lexical"] = (
        "No federal-funds target change is stated; carry the setting known "
        "from the preceding official statement."
    )
    payload["setting_id"] = ""
    return MonetaryPolicySettingV1.from_dict(payload)


def _exact_release_time(text: str) -> tuple[str | None, str | None]:
    match = _EXACT_TIME_RE.search(text)
    if match is None:
        return None, None
    hour = int(match.group(1))
    minute = int(match.group(2))
    if not 1 <= hour <= 12 or not 0 <= minute <= 59:
        raise ValueError("FOMC statement carries an invalid release time")
    if match.group(3).lower() == "p" and hour != 12:
        hour += 12
    if match.group(3).lower() == "a" and hour == 12:
        hour = 0
    return f"{hour:02d}:{minute:02d}", match.group(4).upper()


def parse_federal_reserve_fomc_statement(
    snapshot: OfficialRawSnapshotV1,
    index_entry: FederalReserveFomcIndexEntryV1,
    *,
    previous_setting: MonetaryPolicySettingV1 | None,
) -> FederalReserveFomcDecisionV1:
    """Parse one official statement without inventing an event consensus."""
    if index_entry.statement_uri != snapshot.request.uri:
        raise ValueError("FOMC statement snapshot differs from its index entry")
    artifact = _artifact(snapshot, FomcArtifactRole.STATEMENT)
    text = _visible_html_text(snapshot.content)
    non_rate_direction: MonetaryPolicyDecisionDirection | None = None
    try:
        current = _policy_setting(text)
    except ValueError as exc:
        non_rate_direction = _FOMC_NON_RATE_DIRECTIONS.get(snapshot.request.uri)
        if previous_setting is None or non_rate_direction is None:
            raise ValueError(
                f"FOMC statement has no supported target setting: "
                f"{snapshot.request.uri}"
            ) from exc
        current = _carried_setting(previous_setting)
    previous = (
        _first_previous_setting(text, current)
        if previous_setting is None
        else previous_setting
    )
    release_date = _date_from_uri(snapshot.request.uri)
    local_time, zone = _exact_release_time(text)
    limitations = [
        "No official event-level consensus exists; the expectation is explicitly unavailable.",
        "The policy setting is parsed from the statement's federal-funds target sentence.",
    ]
    if local_time is None:
        limitations.append(
            "The statement supplies a release date but no exact publication clock time."
        )
    if index_entry.action_timing is not MonetaryPolicyActionTiming.SCHEDULED:
        limitations.append(
            "The archive identifies this as an unscheduled, conference-call, or notation action."
        )
    if non_rate_direction is not None:
        limitations.append(
            "This recognized guidance or operational statement does not change the "
            "federal-funds target; the preceding known setting is carried."
        )
    return FederalReserveFomcDecisionV1(
        meeting_key=index_entry.meeting_key,
        release_date=release_date,
        release_time_local=local_time,
        time_zone_abbreviation=zone,
        action_timing=index_entry.action_timing,
        previous_setting=previous,
        new_setting=current,
        direction=(
            non_rate_direction
            if non_rate_direction is not None
            else _direction(previous, current)
        ),
        statement=artifact,
        expectation_unavailable_reason="The Federal Reserve does not publish a point-in-time event consensus for FOMC decisions.",
        limitations=tuple(limitations),
    )


def _validate_document(
    snapshot: OfficialRawSnapshotV1, role: FomcArtifactRole
) -> FederalReserveFomcArtifactV1:
    artifact = _artifact(snapshot, role)
    expected = "minutes" if role is FomcArtifactRole.MINUTES else "projection"
    if artifact.media_type == "text/html":
        text = _visible_html_text(snapshot.content).lower()
    else:
        try:
            reader = PdfReader(BytesIO(snapshot.content), strict=False)
            text = " ".join(page.extract_text() or "" for page in reader.pages)
        except Exception as exc:
            raise ValueError(f"FOMC {role.value} PDF is unreadable") from exc
        text = text.lower()
    if expected not in text:
        raise ValueError(f"FOMC {role.value} artifact lacks expected content")
    return artifact


def build_federal_reserve_fomc_archive_manifest(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    release_index: FederalReserveFomcReleaseIndexV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
) -> FederalReserveFomcArchiveManifestV1:
    """Build all three qualified FOMC program families from retained bytes."""
    if profile.registry_id != registry.registry_id:
        raise ValueError("FOMC profile and registry identities differ")
    try:
        programs = tuple(
            profile.by_key[key]
            for key in (
                FOMC_DECISION_PROGRAM_KEY,
                FOMC_MINUTES_PROGRAM_KEY,
                FOMC_SEP_PROGRAM_KEY,
            )
        )
    except KeyError as exc:
        raise ValueError("FOMC profile omits a required program") from exc
    if any(item.source_key != FOMC_SOURCE_KEY for item in programs):
        raise ValueError("FOMC profile has the wrong source binding")
    if release_index.as_of_date < FOMC_LATEST_PACKAGED_DECISION_DATE:
        raise ValueError(
            "FOMC archive boundary predates the packaged qualification"
        )
    expected_document_uris = {
        uri
        for entry in release_index.entries
        for uri in (entry.statement_uri, entry.minutes_uri, entry.sep_uri)
        if uri is not None
    }
    if set(snapshots_by_uri) != expected_document_uris or any(
        uri != snapshot.request.uri
        for uri, snapshot in snapshots_by_uri.items()
    ):
        raise ValueError("FOMC document snapshot inventory differs")
    entries_with_statements = sorted(
        (
            item
            for item in release_index.entries
            if item.statement_uri is not None
        ),
        key=lambda item: (
            _date_from_uri(cast(str, item.statement_uri)),
            item.meeting_key,
        ),
    )
    decisions: list[FederalReserveFomcDecisionV1] = []
    previous: MonetaryPolicySettingV1 | None = None
    for entry in entries_with_statements:
        uri = cast(str, entry.statement_uri)
        decision = parse_federal_reserve_fomc_statement(
            snapshots_by_uri[uri], entry, previous_setting=previous
        )
        decisions.append(decision)
        previous = decision.new_setting

    minute_entries: dict[str, list[FederalReserveFomcIndexEntryV1]] = (
        defaultdict(list)
    )
    projection_entries: dict[str, list[FederalReserveFomcIndexEntryV1]] = (
        defaultdict(list)
    )
    for entry in release_index.entries:
        if entry.minutes_uri is not None:
            minute_entries[entry.minutes_uri].append(entry)
        if entry.sep_uri is not None:
            projection_entries[entry.sep_uri].append(entry)

    minutes: list[FederalReserveFomcDocumentV1] = []
    for uri, entries in minute_entries.items():
        release_dates = {item.minutes_release_date for item in entries}
        if None in release_dates or len(release_dates) != 1:
            raise ValueError(
                f"FOMC minutes release dates do not reconcile: {uri}"
            )
        minutes.append(
            FederalReserveFomcDocumentV1(
                role=FomcArtifactRole.MINUTES,
                meeting_keys=tuple(item.meeting_key for item in entries),
                meeting_end_date=max(item.meeting_end_date for item in entries),
                release_date=cast(str, next(iter(release_dates))),
                artifact=_validate_document(
                    snapshots_by_uri[uri], FomcArtifactRole.MINUTES
                ),
                limitations=(
                    "One minutes publication may incorporate separately indexed conference calls.",
                    "Minutes are documentary releases and do not carry a numeric event consensus.",
                ),
            )
        )

    projections: list[FederalReserveFomcDocumentV1] = []
    for uri, entries in projection_entries.items():
        if len(entries) != 1:
            raise ValueError("FOMC SEP artifact binds more than one meeting")
        entry = entries[0]
        delayed_addendum = uri == entry.minutes_uri
        release_date = (
            cast(str, entry.minutes_release_date)
            if delayed_addendum
            else entry.meeting_end_date
        )
        timing_note = (
            "Before April 2011, the contemporaneous public SEP is the "
            "minutes addendum released three weeks after the meeting."
            if delayed_addendum
            else "The advance SEP table is the contemporaneous same-day "
            "projection release."
        )
        projections.append(
            FederalReserveFomcDocumentV1(
                role=FomcArtifactRole.PROJECTIONS,
                meeting_keys=(entry.meeting_key,),
                meeting_end_date=entry.meeting_end_date,
                release_date=release_date,
                artifact=_validate_document(
                    snapshots_by_uri[uri], FomcArtifactRole.PROJECTIONS
                ),
                limitations=(
                    timing_note,
                    "The artifact preserves the complete contemporaneous public projection release.",
                    "SEP values are central-bank projections, not monthly event consensus forecasts.",
                ),
            )
        )

    artifacts = (
        *release_index.index_artifacts,
        *(item.statement for item in decisions),
        *(item.artifact for item in minutes),
        *(item.artifact for item in projections),
    )
    raw_by_uri = {item.source_uri: item for item in artifacts}
    return FederalReserveFomcArchiveManifestV1(
        registry_id=registry.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        decisions=tuple(decisions),
        minutes=tuple(minutes),
        projections=tuple(projections),
        raw_artifact_count=len(raw_by_uri),
        total_content_bytes=sum(
            item.content_length for item in raw_by_uri.values()
        ),
        exact_minute_decision_count=sum(
            item.exact_minute for item in decisions
        ),
        unscheduled_decision_count=sum(
            item.action_timing is not MonetaryPolicyActionTiming.SCHEDULED
            for item in decisions
        ),
    )


def replay_federal_reserve_fomc_archive(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    index_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Mapping[str, OfficialRawSnapshotV1],
    expected: FederalReserveFomcArchiveManifestV1,
) -> FederalReserveFomcArchiveManifestV1:
    """Rebuild the full corpus and require deterministic manifest equality."""
    index = parse_federal_reserve_fomc_release_index(
        index_snapshots, as_of_date=expected.release_index.as_of_date
    )
    rebuilt = build_federal_reserve_fomc_archive_manifest(
        registry, profile, index, document_snapshots
    )
    if rebuilt != expected:
        raise ValueError("FOMC retained-corpus replay differs")
    return rebuilt


def federal_reserve_fomc_coverages_from_manifest(
    manifest: FederalReserveFomcArchiveManifestV1,
) -> tuple[UnitedStatesProgramCoverageV1, ...]:
    """Derive complete qualification slices for decision, minutes, and SEP."""
    if not isinstance(manifest, FederalReserveFomcArchiveManifestV1):
        raise TypeError("FOMC coverage requires a v1 manifest")
    index_hashes = tuple(
        item.content_sha256 for item in manifest.release_index.index_artifacts
    )
    decision_count = len(manifest.decisions)
    minutes_count = len(manifest.minutes)
    sep_count = len(manifest.projections)
    return (
        UnitedStatesProgramCoverageV1(
            program_key=FOMC_DECISION_PROGRAM_KEY,
            window_start_date=US_BACKFILL_START_DATE,
            window_end_date=manifest.release_index.as_of_date,
            expected_occurrence_count=decision_count,
            schedule_count=decision_count,
            initial_actual_count=decision_count,
            previous_as_known_count=decision_count,
            revision_count=0,
            exact_minute_count=manifest.exact_minute_decision_count,
            forecast_count=0,
            artifact_sha256s=(
                *index_hashes,
                *(item.statement.content_sha256 for item in manifest.decisions),
            ),
            gap_reasons=(UnitedStatesCoverageGapReason.NO_EVENT_FORECAST,),
            notes=(
                "Each policy setting and prior known setting is reconstructed from the ordered official statements.",
                "Scalar targets and post-December-2008 target ranges retain their native shapes.",
                "Source-authored exact clocks are retained; date-only statements remain bounded rather than invented.",
                "The August 2025 strategy notation vote is not classified as a policy-rate decision.",
            ),
        ),
        UnitedStatesProgramCoverageV1(
            program_key=FOMC_MINUTES_PROGRAM_KEY,
            window_start_date=US_BACKFILL_START_DATE,
            window_end_date=manifest.release_index.as_of_date,
            expected_occurrence_count=minutes_count,
            schedule_count=minutes_count,
            initial_actual_count=minutes_count,
            previous_as_known_count=0,
            revision_count=0,
            exact_minute_count=0,
            forecast_count=0,
            artifact_sha256s=(
                *index_hashes,
                *(item.artifact.content_sha256 for item in manifest.minutes),
            ),
            gap_reasons=(),
            notes=(
                "Occurrences are unique published minutes documents, not every conference-call container.",
                "Shared historical minutes preserve all meeting keys linked by the official archive.",
                "Minutes are documentary events and do not carry numeric actual/previous triplets.",
            ),
        ),
        UnitedStatesProgramCoverageV1(
            program_key=FOMC_SEP_PROGRAM_KEY,
            window_start_date=US_BACKFILL_START_DATE,
            window_end_date=manifest.release_index.as_of_date,
            expected_occurrence_count=sep_count,
            schedule_count=sep_count,
            initial_actual_count=sep_count,
            previous_as_known_count=0,
            revision_count=0,
            exact_minute_count=0,
            forecast_count=sep_count,
            artifact_sha256s=(
                *index_hashes,
                *(
                    item.artifact.content_sha256
                    for item in manifest.projections
                ),
            ),
            gap_reasons=(),
            officially_unavailable_count=0,
            notes=(
                "The SEP program begins with the official October 2007 publication; pre-SEP years are outside the program definition.",
                "Each occurrence preserves the complete participant projection artifact as a central-bank forecast vintage.",
                "SEP projections are not substituted for monthly event consensus forecasts.",
            ),
        ),
    )


def packaged_federal_reserve_fomc_manifest_path() -> Path:
    return Path(__file__).with_name("assets") / "us_fomc_archive_v1.json"


def packaged_federal_reserve_fomc_indexes_path() -> Path:
    return Path(__file__).with_name("assets") / "us_fomc_index_pages_v1.json"


def load_packaged_federal_reserve_fomc_archive_manifest() -> (
    FederalReserveFomcArchiveManifestV1
):
    """Load and validate the packaged compact FOMC manifest."""
    return FederalReserveFomcArchiveManifestV1.from_json(
        packaged_federal_reserve_fomc_manifest_path().read_text(
            encoding="utf-8"
        )
    )


def load_packaged_federal_reserve_fomc_index_pages() -> Mapping[str, bytes]:
    """Load the exact 22 official index pages and verify their manifest locks."""
    try:
        payload = json.loads(
            packaged_federal_reserve_fomc_indexes_path().read_text(
                encoding="utf-8"
            )
        )
        values = {
            str(uri): base64.b64decode(str(value), validate=True)
            for uri, value in _mapping(payload, "FOMC index pages").items()
        }
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("packaged FOMC index-page corpus is invalid") from exc
    manifest = load_packaged_federal_reserve_fomc_archive_manifest()
    expected = {
        item.source_uri: item for item in manifest.release_index.index_artifacts
    }
    if set(values) != set(expected):
        raise ValueError("packaged FOMC index-page inventory differs")
    for uri, content in values.items():
        artifact = expected[uri]
        if (
            len(content) != artifact.content_length
            or hashlib.sha256(content).hexdigest() != artifact.content_sha256
        ):
            raise ValueError(f"packaged FOMC index page differs: {uri}")
    return MappingProxyType(values)


__all__ = [
    "FOMC_ARTIFACT_SCHEMA_VERSION",
    "FOMC_CURRENT_CALENDAR_URI",
    "FOMC_DECISION_PROGRAM_KEY",
    "FOMC_DOCUMENT_SCHEMA_VERSION",
    "FOMC_FIRST_YEAR",
    "FOMC_HISTORICAL_INDEX_URI",
    "FOMC_HISTORICAL_URI_TEMPLATE",
    "FOMC_INDEX_ENTRY_SCHEMA_VERSION",
    "FOMC_LAST_HISTORICAL_YEAR",
    "FOMC_LATEST_PACKAGED_DECISION_DATE",
    "FOMC_MANIFEST_SCHEMA_VERSION",
    "FOMC_MINUTES_PROGRAM_KEY",
    "FOMC_RELEASE_INDEX_SCHEMA_VERSION",
    "FOMC_SEP_PROGRAM_KEY",
    "FOMC_SOURCE_KEY",
    "FederalReserveFomcArchiveManifestV1",
    "FederalReserveFomcArtifactV1",
    "FederalReserveFomcDecisionV1",
    "FederalReserveFomcDocumentV1",
    "FederalReserveFomcIndexEntryV1",
    "FederalReserveFomcReleaseIndexV1",
    "FomcArtifactRole",
    "build_federal_reserve_fomc_archive_manifest",
    "build_federal_reserve_fomc_document_requests",
    "build_federal_reserve_fomc_index_requests",
    "federal_reserve_fomc_coverages_from_manifest",
    "load_packaged_federal_reserve_fomc_archive_manifest",
    "load_packaged_federal_reserve_fomc_index_pages",
    "packaged_federal_reserve_fomc_indexes_path",
    "packaged_federal_reserve_fomc_manifest_path",
    "parse_federal_reserve_fomc_release_index",
    "parse_federal_reserve_fomc_statement",
    "replay_federal_reserve_fomc_archive",
]
