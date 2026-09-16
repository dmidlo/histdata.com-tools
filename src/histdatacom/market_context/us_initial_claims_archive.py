"""Deterministic qualification of the official DOL Initial Claims archive.

The Employment and Training Administration archive enumerates weekly release
artifacts from October 2002 onward.  This module binds the annual archive
pages to every real release, excludes two byte-identical wrong-directory
aliases and one source-authored dummy PDF, and reconstructs the seasonally
adjusted regular-state initial-claims headline without substituting a current
revised series for occurrence evidence.

Raw release bytes remain in an operator-retained directory.  The packaged
manifest contains source hashes, exact release clocks, normalized values, and
explicit archive/publication gaps so the complete corpus can be replayed
without network access.
"""

from __future__ import annotations

import base64
import calendar
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, cast
from urllib.parse import urljoin

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import EconomicTimePrecision
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
    normalize_official_source_timestamp,
)
from histdatacom.market_context.us_backfill import (
    US_BACKFILL_START_DATE,
    UnitedStatesBackfillProfileV1,
    UnitedStatesCoverageGapReason,
    UnitedStatesProgramCoverageV1,
)
from histdatacom.runtime_contracts import JSONValue

INITIAL_CLAIMS_SOURCE_KEY: Final = "us.dol.eta-unemployment-insurance"
INITIAL_CLAIMS_PROGRAM_KEY: Final = "us.dol.initial-claims"
INITIAL_CLAIMS_PARSER_ID: Final = "official.dol-eta-initial-claims.v1"
INITIAL_CLAIMS_ARCHIVE_URI: Final = (
    "https://oui.doleta.gov/unemploy/claims_arch.asp"
)
INITIAL_CLAIMS_ARCHIVE_POST_URI: Final = (
    "https://oui.doleta.gov/unemploy/archive.asp"
)
INITIAL_CLAIMS_FIRST_INDEX_YEAR: Final = 2002
INITIAL_CLAIMS_FIRST_RELEASE_DATE: Final = "2002-10-17"
INITIAL_CLAIMS_LATEST_PACKAGED_RELEASE_DATE: Final = "2026-09-10"
INITIAL_CLAIMS_SOURCE_TIMEZONE: Final = "America/New_York"

INITIAL_CLAIMS_INDEX_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.initial-claims-index-artifact.v1"
)
INITIAL_CLAIMS_INDEX_ENTRY_SCHEMA_VERSION: Final = (
    "histdatacom.initial-claims-index-entry.v1"
)
INITIAL_CLAIMS_EXCLUDED_LINK_SCHEMA_VERSION: Final = (
    "histdatacom.initial-claims-excluded-link.v1"
)
INITIAL_CLAIMS_RELEASE_INDEX_SCHEMA_VERSION: Final = (
    "histdatacom.initial-claims-release-index.v1"
)
INITIAL_CLAIMS_PUBLICATION_SCHEMA_VERSION: Final = (
    "histdatacom.initial-claims-publication.v1"
)
INITIAL_CLAIMS_ARCHIVE_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.initial-claims-archive-manifest.v1"
)
INITIAL_CLAIMS_INDEX_PAGES_SCHEMA_VERSION: Final = (
    "histdatacom.initial-claims-index-pages.v1"
)

MAX_INITIAL_CLAIMS_RELEASES: Final = 2_048
MAX_INITIAL_CLAIMS_INDEX_BYTES: Final = 4 * 1024 * 1024
MAX_INITIAL_CLAIMS_RELEASE_BYTES: Final = 4 * 1024 * 1024
MAX_INITIAL_CLAIMS_TOTAL_BYTES: Final = 512 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MONTH_NUMBER: Final[Mapping[str, int]] = MappingProxyType(
    {
        **{
            name.lower(): number
            for number, name in enumerate(calendar.month_name)
            if name
        },
        **{
            name.lower(): number
            for number, name in enumerate(calendar.month_abbr)
            if name
        },
        "sept": 9,
    }
)
_MONTH_PATTERN: Final = (
    r"January|February|March|April|May|June|July|August|September|"
    r"October|November|December|Jan\.?|Feb\.?|Mar\.?|Apr\.?|Jun\.?|"
    r"Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\."
)
_WRITTEN_DATE_RE = re.compile(
    rf"(?P<month>{_MONTH_PATTERN})\s+(?P<day>\d{{1,2}})\s*,?\s+"
    r"(?P<year>20\d{2})",
    re.IGNORECASE,
)
_INDEX_LINK_RE = re.compile(
    r"(?P<href>/press/(?P<year>20\d{2})/(?P<token>\d{6})"
    r"\.(?P<suffix>html?|asp|pdf))",
    re.IGNORECASE,
)
_FOUR_WEEK_RE = re.compile(r"4\s*-\s*week\s+moving\s+average", re.IGNORECASE)
_REVISION_SENTENCE_RE = re.compile(
    r"The\s+previous\s+week(?:'|’)?s\s+(?:level|figure)\s+was\s+"
    r"revised\s+(?:up|down)(?:\s+by)?\s+\d[\d,]*\s+from\s+"
    r"(?P<old>\d[\d,]*)\s+to\s+(?P<new>\d[\d,]*)",
    re.IGNORECASE,
)


def _fuzzy_word(value: str) -> str:
    return r"\s*".join(re.escape(character) for character in value)


_HEADLINE_RE = re.compile(
    rf"In\s+the\s+week\s+ending\s+"
    rf"(?P<month>{_MONTH_PATTERN})\s*(?P<day>\d{{1,2}}),?\s+the\s+"
    rf"{_fuzzy_word('advance')}\s+figure\s+for\s+seasonally\s+adjusted\s+"
    r"initial\s+claims\s+was\s+(?P<actual>\d[\d,]*),\s+"
    r"(?:(?:an?|a)\s+(?P<direction>increase|decrease)\s+of\s+"
    r"(?P<delta>\d[\d,]*)|(?P<unchanged>unchanged))\s+from\s+the\s+"
    r"previous\s+week(?:'|’)?s\s+(?:(?:revised|unrevised)\s+)?"
    r"(?:level|figure)(?:\s+of\s+(?P<reported>\d[\d,]*))?",
    re.IGNORECASE,
)
_REPORTED_ZONE_RE = re.compile(
    r"8:30\s+A\.?M\.?.{0,80}?\b(?P<zone>EST|EDT|Eastern)\b",
    re.IGNORECASE,
)

_WRONG_DIRECTORY_ALIASES: Final = frozenset(
    {
        "https://oui.doleta.gov/press/2012/010313.asp",
        "https://oui.doleta.gov/press/2012/080113.asp",
    }
)
_DUMMY_ARTIFACT_URI: Final = "https://oui.doleta.gov/press/2014/031514.pdf"
_INDEX_DATE_CORRECTIONS: Final[Mapping[str, str]] = MappingProxyType(
    {"https://oui.doleta.gov/press/2019/010318.pdf": "2019-01-03"}
)
_UNINDEXED_RELEASE_DATES: Final = ("2019-10-17",)
_UNPUBLISHED_RELEASE_DATES: Final = (
    "2025-10-02",
    "2025-10-09",
    "2025-10-16",
    "2025-10-23",
    "2025-10-30",
    "2025-11-06",
    "2025-11-13",
)


class InitialClaimsComparisonBasis(str, Enum):
    """Evidence used for a release's previous-as-known value."""

    PREVIOUS_RELEASE = "previous-release"
    CURRENT_REVISION_SENTENCE = "current-release-revision-sentence"
    UNAVAILABLE = "unavailable"


def _required_text(value: object, name: str) -> str:
    text = str(value).strip()
    if not text or len(text) > 16_384:
        raise ValueError(f"{name} is invalid")
    return text


def _optional_text(value: object | None, name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, name)


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _iso_date(value: object, name: str) -> str:
    text = _required_text(value, name)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != text:
        raise ValueError(f"{name} must be a canonical ISO date")
    return text


def _sha256(value: object, name: str) -> str:
    text = _required_text(value, name)
    if _SHA256_RE.fullmatch(text) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return text


def _bounded_int(value: object, name: str, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= maximum
    ):
        raise ValueError(f"{name} is outside its bound")
    return value


def _positive_int(value: object, name: str, maximum: int) -> int:
    result = _bounded_int(value, name, maximum)
    if result == 0:
        raise ValueError(f"{name} must be positive")
    return result


def _claims_number(value: object, name: str) -> tuple[str, int]:
    lexical = _required_text(value, name)
    if re.fullmatch(r"\d{1,3}(?:,\d{3})*|\d+", lexical) is None:
        raise ValueError(f"{name} is not a claims-count lexical value")
    numeric = int(lexical.replace(",", ""))
    if not 0 <= numeric <= 100_000_000:
        raise ValueError(f"{name} is outside its bound")
    return lexical, numeric


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _enum_value(
    value: str | InitialClaimsComparisonBasis,
) -> InitialClaimsComparisonBasis:
    if isinstance(value, InitialClaimsComparisonBasis):
        return value
    try:
        return InitialClaimsComparisonBasis(str(value))
    except ValueError as exc:
        raise ValueError("unsupported Initial Claims comparison basis") from exc


def _sorted_dates(values: Sequence[object], name: str) -> tuple[str, ...]:
    result = tuple(_iso_date(item, name) for item in values)
    if result != tuple(sorted(set(result))):
        raise ValueError(f"{name} must be unique and sorted")
    return result


@dataclass(frozen=True, slots=True)
class DolInitialClaimsIndexArtifactV1:
    """One retained landing or annual archive page."""

    role: str
    year: int | None
    content_sha256: str
    content_length: int
    artifact_id: str = ""
    schema_version: str = INITIAL_CLAIMS_INDEX_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INITIAL_CLAIMS_INDEX_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported Initial Claims index-artifact schema")
        role = _required_text(self.role, "role")
        if role not in {"archive-landing", "annual-index"}:
            raise ValueError("Initial Claims index-artifact role differs")
        if role == "archive-landing":
            if self.year is not None:
                raise ValueError("archive landing cannot name a year")
        elif (
            not isinstance(self.year, int)
            or isinstance(self.year, bool)
            or not INITIAL_CLAIMS_FIRST_INDEX_YEAR <= self.year <= 2100
        ):
            raise ValueError("annual index year is invalid")
        object.__setattr__(self, "role", role)
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _positive_int(
            self.content_length,
            "content_length",
            MAX_INITIAL_CLAIMS_INDEX_BYTES,
        )
        expected = _stable_id(
            "initial-claims-index-artifact", self.identity_payload()
        )
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("Initial Claims index-artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    @property
    def key(self) -> str:
        return "landing" if self.year is None else f"year:{self.year}"

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role,
            "year": self.year,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> DolInitialClaimsIndexArtifactV1:
        year = data.get("year")
        return cls(
            role=str(data.get("role", "")),
            year=cast(int | None, year),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DolInitialClaimsIndexEntryV1:
    """One canonical release link from an annual ETA archive page."""

    release_date: str
    artifact_uri: str
    source_format: OfficialSourceFormat
    entry_id: str = ""
    schema_version: str = INITIAL_CLAIMS_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INITIAL_CLAIMS_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported Initial Claims index-entry schema")
        release = _iso_date(self.release_date, "release_date")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format not in {
            OfficialSourceFormat.HTML,
            OfficialSourceFormat.PDF,
        }:
            raise ValueError("Initial Claims artifact format differs")
        if not self.artifact_uri.startswith("https://oui.doleta.gov/press/"):
            raise ValueError("Initial Claims artifact URI differs")
        if self.artifact_uri in _WRONG_DIRECTORY_ALIASES or (
            self.artifact_uri == _DUMMY_ARTIFACT_URI
        ):
            raise ValueError("excluded Initial Claims link cannot be canonical")
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "source_format", source_format)
        expected = _stable_id(
            "initial-claims-index-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError("Initial Claims index-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "release_date": self.release_date,
            "artifact_uri": self.artifact_uri,
            "source_format": self.source_format.value,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DolInitialClaimsIndexEntryV1:
        return cls(
            release_date=str(data.get("release_date", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DolInitialClaimsExcludedLinkV1:
    """One indexed alias or dummy artifact excluded with exact evidence."""

    artifact_uri: str
    source_format: OfficialSourceFormat
    reason: str
    content_sha256: str
    content_length: int
    canonical_artifact_uri: str | None = None
    excluded_id: str = ""
    schema_version: str = INITIAL_CLAIMS_EXCLUDED_LINK_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INITIAL_CLAIMS_EXCLUDED_LINK_SCHEMA_VERSION:
            raise ValueError("unsupported Initial Claims excluded-link schema")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format not in {
            OfficialSourceFormat.HTML,
            OfficialSourceFormat.PDF,
        }:
            raise ValueError("excluded Initial Claims format differs")
        reason = _required_text(self.reason, "reason")
        canonical = _optional_text(
            self.canonical_artifact_uri, "canonical_artifact_uri"
        )
        if self.artifact_uri in _WRONG_DIRECTORY_ALIASES:
            if reason != "wrong-directory-byte-identical-alias" or (
                canonical is None
            ):
                raise ValueError("Initial Claims alias evidence differs")
        elif self.artifact_uri == _DUMMY_ARTIFACT_URI:
            if reason != "source-authored-dummy-file" or canonical is not None:
                raise ValueError("Initial Claims dummy evidence differs")
        else:
            raise ValueError("unknown excluded Initial Claims URI")
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "reason", reason)
        object.__setattr__(self, "canonical_artifact_uri", canonical)
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _positive_int(
            self.content_length,
            "content_length",
            MAX_INITIAL_CLAIMS_RELEASE_BYTES,
        )
        expected = _stable_id(
            "initial-claims-excluded-link", self.identity_payload()
        )
        if self.excluded_id and self.excluded_id != expected:
            raise ValueError("Initial Claims excluded-link identity differs")
        object.__setattr__(self, "excluded_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "artifact_uri": self.artifact_uri,
            "source_format": self.source_format.value,
            "reason": self.reason,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "canonical_artifact_uri": self.canonical_artifact_uri,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "excluded_id": self.excluded_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> DolInitialClaimsExcludedLinkV1:
        return cls(
            artifact_uri=str(data.get("artifact_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            reason=str(data.get("reason", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            canonical_artifact_uri=_optional_text(
                data.get("canonical_artifact_uri"),
                "canonical_artifact_uri",
            ),
            excluded_id=str(data.get("excluded_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DolInitialClaimsReleaseIndexV1:
    """As-of interpretation of the retained ETA annual archive pages."""

    as_of_date: str
    index_artifacts: tuple[DolInitialClaimsIndexArtifactV1, ...]
    entries: tuple[DolInitialClaimsIndexEntryV1, ...]
    excluded_uris: tuple[str, ...]
    unindexed_release_dates: tuple[str, ...]
    unpublished_release_dates: tuple[str, ...]
    index_id: str = ""
    schema_version: str = INITIAL_CLAIMS_RELEASE_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INITIAL_CLAIMS_RELEASE_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported Initial Claims release-index schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        artifacts = tuple(self.index_artifacts)
        entries = tuple(self.entries)
        if not artifacts or any(
            not isinstance(item, DolInitialClaimsIndexArtifactV1)
            for item in artifacts
        ):
            raise TypeError("Initial Claims index artifacts are invalid")
        expected_years = tuple(
            range(INITIAL_CLAIMS_FIRST_INDEX_YEAR, int(as_of[:4]) + 1)
        )
        years = tuple(
            cast(int, item.year)
            for item in artifacts
            if item.role == "annual-index"
        )
        if sum(item.role == "archive-landing" for item in artifacts) != 1 or (
            years != expected_years
        ):
            raise ValueError("Initial Claims index-page inventory differs")
        if (
            not entries
            or len(entries) > MAX_INITIAL_CLAIMS_RELEASES
            or any(
                not isinstance(item, DolInitialClaimsIndexEntryV1)
                for item in entries
            )
        ):
            raise TypeError("Initial Claims index entries are invalid")
        dates = tuple(item.release_date for item in entries)
        uris = tuple(item.artifact_uri for item in entries)
        if dates != tuple(sorted(set(dates))) or len(set(uris)) != len(uris):
            raise ValueError("Initial Claims index entries are not canonical")
        if dates[0] != INITIAL_CLAIMS_FIRST_RELEASE_DATE or dates[-1] > as_of:
            raise ValueError("Initial Claims index boundary differs")
        excluded = tuple(sorted(set(self.excluded_uris)))
        if set(excluded) != {*_WRONG_DIRECTORY_ALIASES, _DUMMY_ARTIFACT_URI}:
            raise ValueError("Initial Claims excluded URI inventory differs")
        unindexed = _sorted_dates(
            self.unindexed_release_dates, "unindexed_release_dates"
        )
        unpublished = _sorted_dates(
            self.unpublished_release_dates, "unpublished_release_dates"
        )
        if set(unindexed) & set(unpublished):
            raise ValueError("Initial Claims gap inventories overlap")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "index_artifacts", artifacts)
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "excluded_uris", excluded)
        object.__setattr__(self, "unindexed_release_dates", unindexed)
        object.__setattr__(self, "unpublished_release_dates", unpublished)
        expected = _stable_id(
            "initial-claims-release-index", self.identity_payload()
        )
        if self.index_id and self.index_id != expected:
            raise ValueError("Initial Claims release-index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def all_link_uris(self) -> tuple[str, ...]:
        return tuple(item.artifact_uri for item in self.entries) + tuple(
            self.excluded_uris
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "index_artifacts": [
                item.to_dict() for item in self.index_artifacts
            ],
            "entries": [item.to_dict() for item in self.entries],
            "excluded_uris": list(self.excluded_uris),
            "unindexed_release_dates": list(self.unindexed_release_dates),
            "unpublished_release_dates": list(self.unpublished_release_dates),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> DolInitialClaimsReleaseIndexV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            index_artifacts=tuple(
                DolInitialClaimsIndexArtifactV1.from_dict(
                    _mapping(item, "index artifact")
                )
                for item in _sequence(
                    data.get("index_artifacts"), "index_artifacts"
                )
            ),
            entries=tuple(
                DolInitialClaimsIndexEntryV1.from_dict(
                    _mapping(item, "index entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            excluded_uris=tuple(
                str(item)
                for item in _sequence(
                    data.get("excluded_uris"), "excluded_uris"
                )
            ),
            unindexed_release_dates=tuple(
                str(item)
                for item in _sequence(
                    data.get("unindexed_release_dates"),
                    "unindexed_release_dates",
                )
            ),
            unpublished_release_dates=tuple(
                str(item)
                for item in _sequence(
                    data.get("unpublished_release_dates"),
                    "unpublished_release_dates",
                )
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DolInitialClaimsPublicationV1:
    """One occurrence-specific regular-state initial-claims publication."""

    release_date: str
    reference_week_ending: str
    previous_reference_week_ending: str
    released_lexical: str
    released_at_ns: int
    reported_zone: str
    artifact_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    parser_era: str
    actual_lexical: str
    actual_value: int
    previous_as_known_lexical: str | None
    previous_as_known_value: int | None
    revised_previous_lexical: str
    revised_previous_value: int
    source_reported_prior_lexical: str | None
    source_reported_prior_value: int | None
    comparison_basis: InitialClaimsComparisonBasis
    previous_artifact_uri: str | None
    release_time_locator: str
    value_locator: str
    revision_locator: str
    limitations: tuple[str, ...]
    publication_id: str = ""
    schema_version: str = INITIAL_CLAIMS_PUBLICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INITIAL_CLAIMS_PUBLICATION_SCHEMA_VERSION:
            raise ValueError("unsupported Initial Claims publication schema")
        release = _iso_date(self.release_date, "release_date")
        reference = _iso_date(
            self.reference_week_ending, "reference_week_ending"
        )
        previous_reference = _iso_date(
            self.previous_reference_week_ending,
            "previous_reference_week_ending",
        )
        if date.fromisoformat(previous_reference) + timedelta(days=7) != (
            date.fromisoformat(reference)
        ):
            raise ValueError("Initial Claims previous week is not seven days")
        if not reference < release:
            raise ValueError(
                "Initial Claims reference week must precede release"
            )
        released = _required_text(self.released_lexical, "released_lexical")
        if released != f"{release}T08:30:00":
            raise ValueError("Initial Claims released lexical time differs")
        expected_ns = normalize_official_source_timestamp(
            released,
            INITIAL_CLAIMS_SOURCE_TIMEZONE,
            EconomicTimePrecision.EXACT_MINUTE,
        ).utc_ns
        if self.released_at_ns != expected_ns:
            raise ValueError("Initial Claims released instant differs")
        zone = _required_text(self.reported_zone, "reported_zone")
        if zone.upper() not in {"EST", "EDT", "EASTERN"}:
            raise ValueError("Initial Claims source timezone token differs")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format not in {
            OfficialSourceFormat.HTML,
            OfficialSourceFormat.PDF,
        }:
            raise ValueError("Initial Claims publication format differs")
        if not self.artifact_uri.startswith("https://oui.doleta.gov/press/"):
            raise ValueError("Initial Claims publication URI differs")
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _positive_int(
            self.content_length,
            "content_length",
            MAX_INITIAL_CLAIMS_RELEASE_BYTES,
        )
        actual_lexical, actual = _claims_number(
            self.actual_lexical, "actual_lexical"
        )
        revised_lexical, revised = _claims_number(
            self.revised_previous_lexical,
            "revised_previous_lexical",
        )
        if (
            actual != self.actual_value
            or revised != self.revised_previous_value
        ):
            raise ValueError("Initial Claims lexical and numeric values differ")
        basis = _enum_value(self.comparison_basis)
        previous_uri = _optional_text(
            self.previous_artifact_uri, "previous_artifact_uri"
        )
        prior_lexical = _optional_text(
            self.previous_as_known_lexical,
            "previous_as_known_lexical",
        )
        reported_lexical = _optional_text(
            self.source_reported_prior_lexical,
            "source_reported_prior_lexical",
        )
        if (prior_lexical is None) != (self.previous_as_known_value is None):
            raise ValueError("Initial Claims previous-as-known pair differs")
        if (reported_lexical is None) != (
            self.source_reported_prior_value is None
        ):
            raise ValueError(
                "Initial Claims source-reported prior pair differs"
            )
        if prior_lexical is not None:
            _, prior = _claims_number(
                prior_lexical, "previous_as_known_lexical"
            )
            if prior != self.previous_as_known_value:
                raise ValueError("Initial Claims previous-as-known differs")
        if reported_lexical is not None:
            _, reported = _claims_number(
                reported_lexical, "source_reported_prior_lexical"
            )
            if reported != self.source_reported_prior_value:
                raise ValueError("Initial Claims source-reported prior differs")
        if basis is InitialClaimsComparisonBasis.UNAVAILABLE:
            if prior_lexical is not None or previous_uri is not None:
                raise ValueError(
                    "unavailable comparison carries prior evidence"
                )
        elif prior_lexical is None:
            raise ValueError("comparable Initial Claims release lacks prior")
        elif basis is InitialClaimsComparisonBasis.PREVIOUS_RELEASE:
            if previous_uri is None:
                raise ValueError("previous-release comparison lacks artifact")
        elif previous_uri != self.artifact_uri:
            raise ValueError("current-release comparison evidence differs")
        limitations = tuple(
            _required_text(item, "limitations") for item in self.limitations
        )
        if not limitations:
            raise ValueError("Initial Claims publication limitations required")
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "reference_week_ending", reference)
        object.__setattr__(
            self, "previous_reference_week_ending", previous_reference
        )
        object.__setattr__(self, "released_lexical", released)
        object.__setattr__(self, "reported_zone", zone)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(
            self, "parser_era", _required_text(self.parser_era, "parser_era")
        )
        object.__setattr__(self, "actual_lexical", actual_lexical)
        object.__setattr__(self, "revised_previous_lexical", revised_lexical)
        object.__setattr__(self, "previous_as_known_lexical", prior_lexical)
        object.__setattr__(
            self, "source_reported_prior_lexical", reported_lexical
        )
        object.__setattr__(self, "comparison_basis", basis)
        object.__setattr__(self, "previous_artifact_uri", previous_uri)
        for name in (
            "release_time_locator",
            "value_locator",
            "revision_locator",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "initial-claims-publication", self.identity_payload()
        )
        if self.publication_id and self.publication_id != expected:
            raise ValueError("Initial Claims publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    @property
    def revision_comparable(self) -> bool:
        return self.previous_as_known_value is not None

    @property
    def revision_delta(self) -> int | None:
        if self.previous_as_known_value is None:
            return None
        return self.revised_previous_value - self.previous_as_known_value

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "release_date": self.release_date,
            "reference_week_ending": self.reference_week_ending,
            "previous_reference_week_ending": (
                self.previous_reference_week_ending
            ),
            "released_lexical": self.released_lexical,
            "released_at_ns": self.released_at_ns,
            "reported_zone": self.reported_zone,
            "artifact_uri": self.artifact_uri,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "parser_era": self.parser_era,
            "actual_lexical": self.actual_lexical,
            "actual_value": self.actual_value,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "previous_as_known_value": self.previous_as_known_value,
            "revised_previous_lexical": self.revised_previous_lexical,
            "revised_previous_value": self.revised_previous_value,
            "source_reported_prior_lexical": (
                self.source_reported_prior_lexical
            ),
            "source_reported_prior_value": self.source_reported_prior_value,
            "comparison_basis": self.comparison_basis.value,
            "previous_artifact_uri": self.previous_artifact_uri,
            "release_time_locator": self.release_time_locator,
            "value_locator": self.value_locator,
            "revision_locator": self.revision_locator,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "publication_id": self.publication_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> DolInitialClaimsPublicationV1:
        return cls(
            release_date=str(data.get("release_date", "")),
            reference_week_ending=str(data.get("reference_week_ending", "")),
            previous_reference_week_ending=str(
                data.get("previous_reference_week_ending", "")
            ),
            released_lexical=str(data.get("released_lexical", "")),
            released_at_ns=cast(int, data.get("released_at_ns")),
            reported_zone=str(data.get("reported_zone", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            parser_era=str(data.get("parser_era", "")),
            actual_lexical=str(data.get("actual_lexical", "")),
            actual_value=cast(int, data.get("actual_value")),
            previous_as_known_lexical=_optional_text(
                data.get("previous_as_known_lexical"),
                "previous_as_known_lexical",
            ),
            previous_as_known_value=cast(
                int | None, data.get("previous_as_known_value")
            ),
            revised_previous_lexical=str(
                data.get("revised_previous_lexical", "")
            ),
            revised_previous_value=cast(
                int, data.get("revised_previous_value")
            ),
            source_reported_prior_lexical=_optional_text(
                data.get("source_reported_prior_lexical"),
                "source_reported_prior_lexical",
            ),
            source_reported_prior_value=cast(
                int | None, data.get("source_reported_prior_value")
            ),
            comparison_basis=_enum_value(str(data.get("comparison_basis", ""))),
            previous_artifact_uri=_optional_text(
                data.get("previous_artifact_uri"),
                "previous_artifact_uri",
            ),
            release_time_locator=str(data.get("release_time_locator", "")),
            value_locator=str(data.get("value_locator", "")),
            revision_locator=str(data.get("revision_locator", "")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DolInitialClaimsArchiveManifestV1:
    """Compact replay contract for the complete retained ETA corpus."""

    registry_id: str
    profile_id: str
    release_index: DolInitialClaimsReleaseIndexV1
    publications: tuple[DolInitialClaimsPublicationV1, ...]
    excluded_links: tuple[DolInitialClaimsExcludedLinkV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    html_publication_count: int
    pdf_publication_count: int
    qualified_triplet_count: int
    revision_occurrence_count: int
    noncomparable_revision_count: int
    current_revision_fallback_count: int
    source_prior_mismatch_count: int
    manifest_id: str = ""
    schema_version: str = INITIAL_CLAIMS_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INITIAL_CLAIMS_ARCHIVE_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Initial Claims archive-manifest schema"
            )
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("Initial Claims registry identity differs")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("Initial Claims profile identity differs")
        if not isinstance(self.release_index, DolInitialClaimsReleaseIndexV1):
            raise TypeError("Initial Claims manifest requires a release index")
        publications = tuple(self.publications)
        excluded = tuple(self.excluded_links)
        if (
            not publications
            or len(publications) > MAX_INITIAL_CLAIMS_RELEASES
            or any(
                not isinstance(item, DolInitialClaimsPublicationV1)
                for item in publications
            )
        ):
            raise TypeError("Initial Claims publications are invalid")
        if any(
            not isinstance(item, DolInitialClaimsExcludedLinkV1)
            for item in excluded
        ):
            raise TypeError("Initial Claims excluded links are invalid")
        dates = tuple(item.release_date for item in publications)
        if dates != tuple(sorted(set(dates))):
            raise ValueError("Initial Claims publications are not sorted")
        if dates != tuple(
            item.release_date for item in self.release_index.entries
        ):
            raise ValueError("Initial Claims publications differ from index")
        if {item.artifact_uri for item in excluded} != set(
            self.release_index.excluded_uris
        ):
            raise ValueError("Initial Claims exclusions differ from index")
        html_count = sum(
            item.source_format is OfficialSourceFormat.HTML
            for item in publications
        )
        pdf_count = sum(
            item.source_format is OfficialSourceFormat.PDF
            for item in publications
        )
        qualified = sum(item.revision_comparable for item in publications)
        revisions = sum(
            item.revision_delta != 0
            for item in publications
            if item.revision_delta is not None
        )
        noncomparable = len(publications) - qualified
        fallbacks = sum(
            item.comparison_basis
            is InitialClaimsComparisonBasis.CURRENT_REVISION_SENTENCE
            for item in publications
        )
        mismatches = sum(
            item.source_reported_prior_value is not None
            and item.previous_as_known_value is not None
            and item.source_reported_prior_value != item.previous_as_known_value
            for item in publications
        )
        expected_counts = {
            "html_publication_count": html_count,
            "pdf_publication_count": pdf_count,
            "qualified_triplet_count": qualified,
            "revision_occurrence_count": revisions,
            "noncomparable_revision_count": noncomparable,
            "current_revision_fallback_count": fallbacks,
            "source_prior_mismatch_count": mismatches,
        }
        for name, expected_value in expected_counts.items():
            if getattr(self, name) != expected_value:
                raise ValueError(f"Initial Claims {name} differs")
        if html_count + pdf_count != len(publications):
            raise ValueError("Initial Claims format counts differ")
        index_bytes = sum(
            item.content_length for item in self.release_index.index_artifacts
        )
        release_bytes = sum(item.content_length for item in publications)
        excluded_bytes = sum(item.content_length for item in excluded)
        expected_raw_count = (
            len(self.release_index.index_artifacts)
            + len(publications)
            + len(excluded)
        )
        if self.raw_artifact_count != expected_raw_count:
            raise ValueError("Initial Claims raw-artifact count differs")
        if (
            self.total_content_bytes
            != index_bytes + release_bytes + excluded_bytes
        ):
            raise ValueError("Initial Claims total byte count differs")
        _bounded_int(
            self.total_content_bytes,
            "total_content_bytes",
            MAX_INITIAL_CLAIMS_TOTAL_BYTES,
        )
        all_hashes = {
            *(
                item.content_sha256
                for item in self.release_index.index_artifacts
            ),
            *(item.content_sha256 for item in publications),
            *(item.content_sha256 for item in excluded),
        }
        if self.unique_content_sha256_count != len(all_hashes):
            raise ValueError("Initial Claims unique hash count differs")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "publications", publications)
        object.__setattr__(self, "excluded_links", excluded)
        expected = _stable_id(
            "initial-claims-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("Initial Claims archive-manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def by_release_date(self) -> Mapping[str, DolInitialClaimsPublicationV1]:
        return MappingProxyType(
            {item.release_date: item for item in self.publications}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "publications": [item.to_dict() for item in self.publications],
            "excluded_links": [item.to_dict() for item in self.excluded_links],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "html_publication_count": self.html_publication_count,
            "pdf_publication_count": self.pdf_publication_count,
            "qualified_triplet_count": self.qualified_triplet_count,
            "revision_occurrence_count": self.revision_occurrence_count,
            "noncomparable_revision_count": self.noncomparable_revision_count,
            "current_revision_fallback_count": (
                self.current_revision_fallback_count
            ),
            "source_prior_mismatch_count": self.source_prior_mismatch_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> DolInitialClaimsArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=DolInitialClaimsReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            publications=tuple(
                DolInitialClaimsPublicationV1.from_dict(
                    _mapping(item, "publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            excluded_links=tuple(
                DolInitialClaimsExcludedLinkV1.from_dict(
                    _mapping(item, "excluded link")
                )
                for item in _sequence(
                    data.get("excluded_links"), "excluded_links"
                )
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            html_publication_count=cast(
                int, data.get("html_publication_count")
            ),
            pdf_publication_count=cast(int, data.get("pdf_publication_count")),
            qualified_triplet_count=cast(
                int, data.get("qualified_triplet_count")
            ),
            revision_occurrence_count=cast(
                int, data.get("revision_occurrence_count")
            ),
            noncomparable_revision_count=cast(
                int, data.get("noncomparable_revision_count")
            ),
            current_revision_fallback_count=cast(
                int, data.get("current_revision_fallback_count")
            ),
            source_prior_mismatch_count=cast(
                int, data.get("source_prior_mismatch_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> DolInitialClaimsArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "Initial Claims archive manifest is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload, "archive manifest"))


class _VisibleHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag.lower() in {"script", "style"}:
            self._skip_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._skip_depth:
            self.parts.append(data)

    @property
    def visible_text(self) -> str:
        return " ".join(" ".join(self.parts).replace("\xa0", " ").split())


@dataclass(frozen=True, slots=True)
class _ParsedInitialClaimsRelease:
    release_date: str
    reference_week_ending: str
    reported_zone: str
    actual_lexical: str
    actual_value: int
    revised_previous_lexical: str
    revised_previous_value: int
    source_reported_prior_lexical: str | None
    source_reported_prior_value: int | None
    parser_era: str


def _request(
    registry: OfficialSourceRegistryV1,
    *,
    method: OfficialRequestMethod,
    uri: str,
    source_format: OfficialSourceFormat,
    window_start: str,
    window_end: str,
    body_text: str | None = None,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(INITIAL_CLAIMS_SOURCE_KEY)
    if source.parser_id != INITIAL_CLAIMS_PARSER_ID:
        raise ValueError("Initial Claims source parser differs")
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=method,
        uri=uri,
        source_format=source_format,
        body_text=body_text,
        body_content_type=(
            "application/x-www-form-urlencoded"
            if body_text is not None
            else None
        ),
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=window_start,
        window_end=window_end,
        page_number=page_number,
    )


def build_dol_initial_claims_index_requests(
    registry: OfficialSourceRegistryV1, *, as_of_date: str
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan the landing page and every annual ETA archive POST."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("Initial Claims requests require a v1 registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    requests = [
        _request(
            registry,
            method=OfficialRequestMethod.GET,
            uri=INITIAL_CLAIMS_ARCHIVE_URI,
            source_format=OfficialSourceFormat.HTML,
            window_start=INITIAL_CLAIMS_FIRST_RELEASE_DATE,
            window_end=as_of,
        )
    ]
    for year in range(INITIAL_CLAIMS_FIRST_INDEX_YEAR, int(as_of[:4]) + 1):
        requests.append(
            _request(
                registry,
                method=OfficialRequestMethod.POST,
                uri=INITIAL_CLAIMS_ARCHIVE_POST_URI,
                source_format=OfficialSourceFormat.HTML,
                window_start=f"{year:04d}-01-01",
                window_end=min(as_of, f"{year:04d}-12-31"),
                body_text=f"report=press&year={year}&submit=Submit",
                page_number=year - INITIAL_CLAIMS_FIRST_INDEX_YEAR + 1,
            )
        )
    return tuple(requests)


def _validate_index_snapshot(snapshot: OfficialRawSnapshotV1) -> None:
    if (
        not isinstance(snapshot, OfficialRawSnapshotV1)
        or snapshot.request.source_key != INITIAL_CLAIMS_SOURCE_KEY
        or snapshot.request.parser_id != INITIAL_CLAIMS_PARSER_ID
        or snapshot.request.source_format is not OfficialSourceFormat.HTML
        or snapshot.status_code != 200
        or not snapshot.content
        or len(snapshot.content) > MAX_INITIAL_CLAIMS_INDEX_BYTES
    ):
        raise ValueError("Initial Claims index snapshot differs")


def _index_artifact(
    snapshot: OfficialRawSnapshotV1,
) -> DolInitialClaimsIndexArtifactV1:
    year = (
        None
        if snapshot.request.page_number is None
        else int(cast(str, snapshot.request.window_start)[:4])
    )
    return DolInitialClaimsIndexArtifactV1(
        role=(
            "archive-landing"
            if snapshot.request.page_number is None
            else "annual-index"
        ),
        year=year,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
    )


def _date_from_index_link(match: re.Match[str]) -> str:
    token = match.group("token")
    year = 2000 + int(token[4:])
    return date(year, int(token[:2]), int(token[2:4])).isoformat()


def parse_dol_initial_claims_release_index(
    snapshots: Sequence[OfficialRawSnapshotV1], *, as_of_date: str
) -> DolInitialClaimsReleaseIndexV1:
    """Parse canonical release links and preserve every known archive gap."""
    as_of = _iso_date(as_of_date, "as_of_date")
    values = tuple(snapshots)
    for snapshot in values:
        _validate_index_snapshot(snapshot)
    landing = tuple(item for item in values if item.request.page_number is None)
    years = {
        int(cast(str, item.request.window_start)[:4]): item
        for item in values
        if item.request.page_number is not None
    }
    expected_years = set(
        range(INITIAL_CLAIMS_FIRST_INDEX_YEAR, int(as_of[:4]) + 1)
    )
    if len(landing) != 1 or set(years) != expected_years:
        raise ValueError("Initial Claims index snapshot inventory differs")
    try:
        landing_text = landing[0].content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("Initial Claims landing page is not UTF-8") from exc
    landing_parser = _VisibleHtmlParser()
    landing_parser.feed(landing_text)
    landing_parser.close()
    visible = landing_parser.visible_text.lower()
    if (
        "prior versions of the news release" not in visible
        or "8:30am est" not in visible
    ):
        raise ValueError("Initial Claims landing-page semantics differ")
    entries: list[DolInitialClaimsIndexEntryV1] = []
    excluded: set[str] = set()
    artifacts = [_index_artifact(landing[0])]
    for year in sorted(expected_years):
        snapshot = years[year]
        artifacts.append(_index_artifact(snapshot))
        try:
            text = snapshot.content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"Initial Claims {year} index is not UTF-8"
            ) from exc
        matches = tuple(_INDEX_LINK_RE.finditer(text))
        if not matches:
            raise ValueError(f"Initial Claims {year} index has no releases")
        for match in matches:
            uri = urljoin("https://oui.doleta.gov", match.group("href"))
            indexed_date = _INDEX_DATE_CORRECTIONS.get(
                uri, _date_from_index_link(match)
            )
            if indexed_date > as_of:
                continue
            if uri in _WRONG_DIRECTORY_ALIASES or uri == _DUMMY_ARTIFACT_URI:
                excluded.add(uri)
                continue
            if int(indexed_date[:4]) != year:
                raise ValueError("Initial Claims index link year differs")
            source_format = (
                OfficialSourceFormat.PDF
                if match.group("suffix").lower() == "pdf"
                else OfficialSourceFormat.HTML
            )
            entries.append(
                DolInitialClaimsIndexEntryV1(
                    release_date=indexed_date,
                    artifact_uri=uri,
                    source_format=source_format,
                )
            )
    return DolInitialClaimsReleaseIndexV1(
        as_of_date=as_of,
        index_artifacts=tuple(artifacts),
        entries=tuple(sorted(entries, key=lambda item: item.release_date)),
        excluded_uris=tuple(sorted(excluded)),
        unindexed_release_dates=tuple(
            item for item in _UNINDEXED_RELEASE_DATES if item <= as_of
        ),
        unpublished_release_dates=tuple(
            item for item in _UNPUBLISHED_RELEASE_DATES if item <= as_of
        ),
    )


def _format_from_uri(uri: str) -> OfficialSourceFormat:
    return (
        OfficialSourceFormat.PDF
        if uri.lower().endswith(".pdf")
        else OfficialSourceFormat.HTML
    )


def _date_from_release_uri(uri: str) -> str:
    corrected = _INDEX_DATE_CORRECTIONS.get(uri)
    if corrected is not None:
        return corrected
    match = _INDEX_LINK_RE.search(uri.replace("https://oui.doleta.gov", ""))
    if match is None:
        raise ValueError("Initial Claims release URI differs")
    return _date_from_index_link(match)


def build_dol_initial_claims_release_requests(
    registry: OfficialSourceRegistryV1,
    release_index: DolInitialClaimsReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan all canonical and explicitly excluded indexed artifacts."""
    if not isinstance(release_index, DolInitialClaimsReleaseIndexV1):
        raise TypeError("Initial Claims releases require a v1 index")
    return tuple(
        _request(
            registry,
            method=OfficialRequestMethod.GET,
            uri=uri,
            source_format=_format_from_uri(uri),
            window_start=_date_from_release_uri(uri),
            window_end=_date_from_release_uri(uri),
        )
        for uri in release_index.all_link_uris
    )


def _decode_html(content: bytes) -> str:
    for encoding in ("utf-8-sig", "windows-1252", "latin-1"):
        try:
            text = content.decode(encoding)
        except UnicodeDecodeError:
            continue
        parser = _VisibleHtmlParser()
        parser.feed(text)
        parser.close()
        return parser.visible_text
    raise ValueError("Initial Claims HTML encoding is unsupported")


def _pdf_text(content: bytes, *, layout: bool = False) -> str:
    if not content.startswith(b"%PDF-"):
        raise ValueError("Initial Claims PDF lacks a signature")
    try:
        reader = PdfReader(BytesIO(content))
        pages = reader.pages[:2]
        values = [
            (
                page.extract_text(extraction_mode="layout")
                if layout
                else page.extract_text()
            )
            or ""
            for page in pages
        ]
    except Exception as exc:
        raise ValueError("Initial Claims PDF extraction failed") from exc
    return " ".join(" ".join(values).replace("\xa0", " ").split())


def _month_number(value: str) -> int:
    result = _MONTH_NUMBER.get(value.rstrip(".").lower())
    if result is None:
        raise ValueError("Initial Claims month token differs")
    return result


def _parse_written_date(match: re.Match[str]) -> str:
    return date(
        int(match.group("year")),
        _month_number(match.group("month")),
        int(match.group("day")),
    ).isoformat()


def _parse_release_text(
    text: str,
    *,
    indexed_release_date: str,
    parser_era: str,
) -> _ParsedInitialClaimsRelease:
    time_offset = text.lower().find("8:30")
    if time_offset < 0:
        raise ValueError("Initial Claims release clock is unavailable")
    date_match = _WRITTEN_DATE_RE.search(
        text, time_offset, min(len(text), time_offset + 600)
    )
    if date_match is None:
        raise ValueError("Initial Claims release header date is unavailable")
    release_date = _parse_written_date(date_match)
    if release_date != indexed_release_date:
        raise ValueError("Initial Claims index and header dates differ")
    zone_match = _REPORTED_ZONE_RE.search(
        text, time_offset, min(len(text), time_offset + 160)
    )
    if zone_match is None:
        raise ValueError("Initial Claims reported timezone is unavailable")
    headline = _HEADLINE_RE.search(text)
    if headline is None:
        raise ValueError("Initial Claims headline is unavailable")
    actual_lexical, actual = _claims_number(headline.group("actual"), "actual")
    delta = int((headline.group("delta") or "0").replace(",", ""))
    direction = (headline.group("direction") or "").lower()
    signed_delta = (
        delta
        if direction == "increase"
        else -delta if direction == "decrease" else 0
    )
    revised = actual - signed_delta
    revised_lexical = f"{revised:,}"
    reference_month = _month_number(headline.group("month"))
    release = date.fromisoformat(release_date)
    reference_year = (
        release.year - 1
        if reference_month == 12 and release.month == 1
        else release.year
    )
    reference = date(
        reference_year,
        reference_month,
        int(headline.group("day")),
    ).isoformat()
    average_match = _FOUR_WEEK_RE.search(text, headline.end())
    revision_block = text[
        headline.end() : (
            average_match.start()
            if average_match is not None
            else min(len(text), headline.end() + 600)
        )
    ]
    revision_match = _REVISION_SENTENCE_RE.search(revision_block)
    reported_lexical: str | None = None
    reported_value: int | None = None
    if revision_match is not None:
        reported_lexical, reported_value = _claims_number(
            revision_match.group("old"), "source reported prior"
        )
        _, reported_revised = _claims_number(
            revision_match.group("new"), "source reported revised prior"
        )
        if reported_revised != revised:
            raise ValueError("Initial Claims revision sentence differs")
    return _ParsedInitialClaimsRelease(
        release_date=release_date,
        reference_week_ending=reference,
        reported_zone=zone_match.group("zone"),
        actual_lexical=actual_lexical,
        actual_value=actual,
        revised_previous_lexical=revised_lexical,
        revised_previous_value=revised,
        source_reported_prior_lexical=reported_lexical,
        source_reported_prior_value=reported_value,
        parser_era=parser_era,
    )


def _parse_release(
    snapshot: OfficialRawSnapshotV1,
    indexed: DolInitialClaimsIndexEntryV1,
) -> _ParsedInitialClaimsRelease:
    if (
        not isinstance(snapshot, OfficialRawSnapshotV1)
        or snapshot.request.source_key != INITIAL_CLAIMS_SOURCE_KEY
        or snapshot.request.parser_id != INITIAL_CLAIMS_PARSER_ID
        or snapshot.request.uri != indexed.artifact_uri
        or snapshot.request.source_format is not indexed.source_format
        or snapshot.status_code != 200
        or not snapshot.content
        or len(snapshot.content) > MAX_INITIAL_CLAIMS_RELEASE_BYTES
    ):
        raise ValueError("Initial Claims release snapshot differs")
    if indexed.source_format is OfficialSourceFormat.HTML:
        parser_era = (
            "legacy-html"
            if indexed.artifact_uri.lower().endswith((".html", ".htm"))
            else "legacy-asp"
        )
        text = _decode_html(snapshot.content)
        return _parse_release_text(
            text,
            indexed_release_date=indexed.release_date,
            parser_era=parser_era,
        )
    text = _pdf_text(snapshot.content)
    try:
        return _parse_release_text(
            text,
            indexed_release_date=indexed.release_date,
            parser_era="native-pdf",
        )
    except ValueError:
        return _parse_release_text(
            _pdf_text(snapshot.content, layout=True),
            indexed_release_date=indexed.release_date,
            parser_era="native-pdf-layout-fallback",
        )


def _excluded_link(
    uri: str,
    snapshot: OfficialRawSnapshotV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
) -> DolInitialClaimsExcludedLinkV1:
    if uri in _WRONG_DIRECTORY_ALIASES:
        canonical_uri = uri.replace("/press/2012/", "/press/2013/")
        canonical = snapshots_by_uri.get(canonical_uri)
        if canonical is None or canonical.content != snapshot.content:
            raise ValueError("Initial Claims wrong-directory alias differs")
        return DolInitialClaimsExcludedLinkV1(
            artifact_uri=uri,
            source_format=OfficialSourceFormat.HTML,
            reason="wrong-directory-byte-identical-alias",
            content_sha256=snapshot.content_sha256,
            content_length=len(snapshot.content),
            canonical_artifact_uri=canonical_uri,
        )
    text = _pdf_text(snapshot.content)
    if "Dummy file" not in text or "March 15, 2014" not in text:
        raise ValueError("Initial Claims dummy artifact differs")
    return DolInitialClaimsExcludedLinkV1(
        artifact_uri=uri,
        source_format=OfficialSourceFormat.PDF,
        reason="source-authored-dummy-file",
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
    )


def _publication(
    indexed: DolInitialClaimsIndexEntryV1,
    snapshot: OfficialRawSnapshotV1,
    parsed: _ParsedInitialClaimsRelease,
    by_reference: Mapping[
        str,
        tuple[
            DolInitialClaimsIndexEntryV1,
            OfficialRawSnapshotV1,
            _ParsedInitialClaimsRelease,
        ],
    ],
) -> DolInitialClaimsPublicationV1:
    previous_reference = (
        date.fromisoformat(parsed.reference_week_ending) - timedelta(days=7)
    ).isoformat()
    predecessor = by_reference.get(previous_reference)
    previous_lexical: str | None = None
    previous_value: int | None = None
    previous_uri: str | None = None
    basis = InitialClaimsComparisonBasis.UNAVAILABLE
    limitations = [
        "The measure is regular-state seasonally adjusted initial claims in persons.",
        "No official event-level consensus is published; forecasts remain unavailable.",
    ]
    if predecessor is not None:
        previous_lexical = predecessor[2].actual_lexical
        previous_value = predecessor[2].actual_value
        previous_uri = predecessor[0].artifact_uri
        basis = InitialClaimsComparisonBasis.PREVIOUS_RELEASE
    elif parsed.source_reported_prior_lexical is not None:
        previous_lexical = parsed.source_reported_prior_lexical
        previous_value = parsed.source_reported_prior_value
        previous_uri = indexed.artifact_uri
        basis = InitialClaimsComparisonBasis.CURRENT_REVISION_SENTENCE
        limitations.append(
            "The 2019-10-17 ETA archive artifact is absent; this release's explicit from/to revision sentence preserves that occurrence's initial value for the comparison."
        )
    else:
        limitations.append(
            "A matching prior occurrence artifact/value is unavailable; the actual and source-revised prior remain retained but the revision is noncomparable."
        )
    if (
        parsed.source_reported_prior_value is not None
        and previous_value is not None
        and parsed.source_reported_prior_value != previous_value
    ):
        limitations.append(
            "The current release's annual seasonal-factor comparison basis differs from the preceding occurrence's published actual; previous-as-known remains the preceding occurrence value."
        )
    released_lexical = f"{parsed.release_date}T08:30:00"
    released_at_ns = normalize_official_source_timestamp(
        released_lexical,
        INITIAL_CLAIMS_SOURCE_TIMEZONE,
        EconomicTimePrecision.EXACT_MINUTE,
    ).utc_ns
    return DolInitialClaimsPublicationV1(
        release_date=parsed.release_date,
        reference_week_ending=parsed.reference_week_ending,
        previous_reference_week_ending=previous_reference,
        released_lexical=released_lexical,
        released_at_ns=released_at_ns,
        reported_zone=parsed.reported_zone,
        artifact_uri=indexed.artifact_uri,
        source_format=indexed.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        parser_era=parsed.parser_era,
        actual_lexical=parsed.actual_lexical,
        actual_value=parsed.actual_value,
        previous_as_known_lexical=previous_lexical,
        previous_as_known_value=previous_value,
        revised_previous_lexical=parsed.revised_previous_lexical,
        revised_previous_value=parsed.revised_previous_value,
        source_reported_prior_lexical=parsed.source_reported_prior_lexical,
        source_reported_prior_value=parsed.source_reported_prior_value,
        comparison_basis=basis,
        previous_artifact_uri=previous_uri,
        release_time_locator="occurrence release embargo header",
        value_locator="seasonally adjusted initial-claims headline sentence",
        revision_locator=(
            "current headline change arithmetic and preceding occurrence actual"
            if basis is InitialClaimsComparisonBasis.PREVIOUS_RELEASE
            else (
                "current headline change arithmetic and explicit from/to revision sentence"
                if basis
                is InitialClaimsComparisonBasis.CURRENT_REVISION_SENTENCE
                else "current headline change arithmetic; comparison unavailable"
            )
        ),
        limitations=tuple(limitations),
    )


def build_dol_initial_claims_archive_manifest(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    release_index: DolInitialClaimsReleaseIndexV1,
    index_snapshots: Sequence[OfficialRawSnapshotV1],
    release_snapshots: Sequence[OfficialRawSnapshotV1],
) -> DolInitialClaimsArchiveManifestV1:
    """Build the compact manifest from every retained ETA artifact."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("Initial Claims manifest requires a v1 registry")
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("Initial Claims manifest requires a v1 profile")
    if not isinstance(release_index, DolInitialClaimsReleaseIndexV1):
        raise TypeError("Initial Claims manifest requires a v1 release index")
    if profile.registry_id != registry.registry_id:
        raise ValueError("Initial Claims registry and profile differ")
    program = profile.by_key.get(INITIAL_CLAIMS_PROGRAM_KEY)
    if program is None or program.source_key != INITIAL_CLAIMS_SOURCE_KEY:
        raise ValueError("Initial Claims program source differs")
    rebuilt_index = parse_dol_initial_claims_release_index(
        index_snapshots, as_of_date=release_index.as_of_date
    )
    if rebuilt_index != release_index:
        raise ValueError("Initial Claims retained index replay differs")
    snapshots_by_uri: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in release_snapshots:
        uri = snapshot.request.uri
        if uri in snapshots_by_uri:
            raise ValueError("Initial Claims release snapshots repeat a URI")
        snapshots_by_uri[uri] = snapshot
    if set(snapshots_by_uri) != set(release_index.all_link_uris):
        raise ValueError("Initial Claims release snapshot inventory differs")
    parsed_rows: list[
        tuple[
            DolInitialClaimsIndexEntryV1,
            OfficialRawSnapshotV1,
            _ParsedInitialClaimsRelease,
        ]
    ] = []
    for indexed in release_index.entries:
        snapshot = snapshots_by_uri[indexed.artifact_uri]
        parsed_rows.append(
            (indexed, snapshot, _parse_release(snapshot, indexed))
        )
    by_reference: dict[
        str,
        tuple[
            DolInitialClaimsIndexEntryV1,
            OfficialRawSnapshotV1,
            _ParsedInitialClaimsRelease,
        ],
    ] = {}
    for row in parsed_rows:
        reference = row[2].reference_week_ending
        if reference in by_reference:
            raise ValueError("Initial Claims repeats a reference week")
        by_reference[reference] = row
    publications = tuple(
        _publication(indexed, snapshot, parsed, by_reference)
        for indexed, snapshot, parsed in parsed_rows
    )
    excluded = tuple(
        _excluded_link(uri, snapshots_by_uri[uri], snapshots_by_uri)
        for uri in release_index.excluded_uris
    )
    all_hashes = {
        *(item.content_sha256 for item in release_index.index_artifacts),
        *(item.content_sha256 for item in publications),
        *(item.content_sha256 for item in excluded),
    }
    return DolInitialClaimsArchiveManifestV1(
        registry_id=registry.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        publications=publications,
        excluded_links=excluded,
        raw_artifact_count=(
            len(release_index.index_artifacts)
            + len(publications)
            + len(excluded)
        ),
        unique_content_sha256_count=len(all_hashes),
        total_content_bytes=(
            sum(item.content_length for item in release_index.index_artifacts)
            + sum(item.content_length for item in publications)
            + sum(item.content_length for item in excluded)
        ),
        html_publication_count=sum(
            item.source_format is OfficialSourceFormat.HTML
            for item in publications
        ),
        pdf_publication_count=sum(
            item.source_format is OfficialSourceFormat.PDF
            for item in publications
        ),
        qualified_triplet_count=sum(
            item.revision_comparable for item in publications
        ),
        revision_occurrence_count=sum(
            item.revision_delta != 0
            for item in publications
            if item.revision_delta is not None
        ),
        noncomparable_revision_count=sum(
            not item.revision_comparable for item in publications
        ),
        current_revision_fallback_count=sum(
            item.comparison_basis
            is InitialClaimsComparisonBasis.CURRENT_REVISION_SENTENCE
            for item in publications
        ),
        source_prior_mismatch_count=sum(
            item.source_reported_prior_value is not None
            and item.previous_as_known_value is not None
            and item.source_reported_prior_value != item.previous_as_known_value
            for item in publications
        ),
    )


def replay_dol_initial_claims_archive(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    index_snapshots: Sequence[OfficialRawSnapshotV1],
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    expected: DolInitialClaimsArchiveManifestV1,
) -> DolInitialClaimsArchiveManifestV1:
    """Rebuild and compare the complete retained Initial Claims corpus."""
    rebuilt_index = parse_dol_initial_claims_release_index(
        index_snapshots, as_of_date=expected.release_index.as_of_date
    )
    rebuilt = build_dol_initial_claims_archive_manifest(
        registry,
        profile,
        rebuilt_index,
        index_snapshots,
        release_snapshots,
    )
    if rebuilt != expected:
        raise ValueError("Initial Claims retained-corpus replay differs")
    return rebuilt


def dol_initial_claims_coverage_from_manifest(
    profile: UnitedStatesBackfillProfileV1,
    manifest: DolInitialClaimsArchiveManifestV1,
) -> UnitedStatesProgramCoverageV1:
    """Derive quantified qualified coverage from the verified manifest."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("Initial Claims coverage requires a v1 profile")
    if not isinstance(manifest, DolInitialClaimsArchiveManifestV1):
        raise TypeError("Initial Claims coverage requires a v1 manifest")
    if (
        manifest.profile_id != profile.profile_id
        or manifest.registry_id != profile.registry_id
    ):
        raise ValueError("Initial Claims manifest differs from the profile")
    program = profile.by_key[INITIAL_CLAIMS_PROGRAM_KEY]
    qualified = tuple(
        item for item in manifest.publications if item.revision_comparable
    )
    count = len(qualified)
    coverage = UnitedStatesProgramCoverageV1(
        program_key=INITIAL_CLAIMS_PROGRAM_KEY,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=manifest.release_index.as_of_date,
        expected_occurrence_count=count,
        schedule_count=count,
        initial_actual_count=count,
        previous_as_known_count=count,
        revision_count=manifest.revision_occurrence_count,
        exact_minute_count=count,
        forecast_count=0,
        artifact_sha256s=tuple(item.content_sha256 for item in qualified),
        gap_reasons=(UnitedStatesCoverageGapReason.NO_EVENT_FORECAST,),
        notes=(
            f"Release index: {manifest.release_index.index_id}",
            f"Archive manifest: {manifest.manifest_id}",
            "ETA's occurrence archive begins 2002-10-17; the first retained release is predecessor-only because its preceding occurrence is outside the archive.",
            "The ETA index omits 2019-10-17; the 2019-10-24 release's explicit from/to sentence preserves the previous-as-known comparison for its own triplet.",
            "Seven releases from 2025-10-02 through 2025-11-13 were not published during the appropriations lapse; 2025-11-20 is retained as actual/revised evidence but is noncomparable.",
            "Two byte-identical 2013 releases listed under 2012 and one 2014 dummy PDF are retained as exclusions rather than counted as occurrences.",
        ),
    )
    if not coverage.is_complete_for(program):
        raise ValueError("Initial Claims manifest does not qualify coverage")
    return coverage


def packaged_dol_initial_claims_manifest_path() -> Path:
    return (
        Path(__file__).with_name("assets") / "us_initial_claims_archive_v1.json"
    )


def packaged_dol_initial_claims_indexes_path() -> Path:
    return (
        Path(__file__).with_name("assets")
        / "us_initial_claims_index_pages_v1.json"
    )


def load_packaged_dol_initial_claims_archive_manifest() -> (
    DolInitialClaimsArchiveManifestV1
):
    """Load and validate the compact packaged Initial Claims manifest."""
    try:
        text = packaged_dol_initial_claims_manifest_path().read_text(
            encoding="utf-8"
        )
    except OSError as exc:
        raise ValueError(
            "packaged Initial Claims manifest is unavailable"
        ) from exc
    return DolInitialClaimsArchiveManifestV1.from_json(text)


def load_packaged_dol_initial_claims_index_pages() -> Mapping[str, bytes]:
    """Decode and hash-check every packaged ETA archive index page."""
    try:
        payload = json.loads(
            packaged_dol_initial_claims_indexes_path().read_text(
                encoding="utf-8"
            )
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "packaged Initial Claims index pages are invalid"
        ) from exc
    root = _mapping(payload, "Initial Claims index pages")
    if root.get("schema_version") != INITIAL_CLAIMS_INDEX_PAGES_SCHEMA_VERSION:
        raise ValueError("Initial Claims index-page schema differs")
    pages = _sequence(root.get("pages"), "Initial Claims index pages")
    result: dict[str, bytes] = {}
    for item in pages:
        row = _mapping(item, "Initial Claims index page")
        key = _required_text(row.get("key"), "key")
        encoded = row.get("content_base64")
        if not isinstance(encoded, str):
            raise TypeError("Initial Claims index content must be base64 text")
        try:
            content = base64.b64decode(encoded, validate=True)
        except ValueError as exc:
            raise ValueError("Initial Claims index base64 is invalid") from exc
        if key in result:
            raise ValueError("Initial Claims index pages repeat a key")
        result[key] = content
    manifest = load_packaged_dol_initial_claims_archive_manifest()
    expected = {
        item.key: item for item in manifest.release_index.index_artifacts
    }
    if set(result) != set(expected):
        raise ValueError("Initial Claims packaged index inventory differs")
    for key, artifact in expected.items():
        content = result[key]
        if (
            len(content) != artifact.content_length
            or hashlib.sha256(content).hexdigest() != artifact.content_sha256
        ):
            raise ValueError("Initial Claims packaged index hash differs")
    return MappingProxyType(result)


__all__ = [
    "INITIAL_CLAIMS_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "INITIAL_CLAIMS_ARCHIVE_POST_URI",
    "INITIAL_CLAIMS_ARCHIVE_URI",
    "INITIAL_CLAIMS_EXCLUDED_LINK_SCHEMA_VERSION",
    "INITIAL_CLAIMS_FIRST_RELEASE_DATE",
    "INITIAL_CLAIMS_INDEX_ARTIFACT_SCHEMA_VERSION",
    "INITIAL_CLAIMS_INDEX_ENTRY_SCHEMA_VERSION",
    "INITIAL_CLAIMS_INDEX_PAGES_SCHEMA_VERSION",
    "INITIAL_CLAIMS_LATEST_PACKAGED_RELEASE_DATE",
    "INITIAL_CLAIMS_PARSER_ID",
    "INITIAL_CLAIMS_PROGRAM_KEY",
    "INITIAL_CLAIMS_PUBLICATION_SCHEMA_VERSION",
    "INITIAL_CLAIMS_RELEASE_INDEX_SCHEMA_VERSION",
    "INITIAL_CLAIMS_SOURCE_KEY",
    "DolInitialClaimsArchiveManifestV1",
    "DolInitialClaimsExcludedLinkV1",
    "DolInitialClaimsIndexArtifactV1",
    "DolInitialClaimsIndexEntryV1",
    "DolInitialClaimsPublicationV1",
    "DolInitialClaimsReleaseIndexV1",
    "InitialClaimsComparisonBasis",
    "build_dol_initial_claims_archive_manifest",
    "build_dol_initial_claims_index_requests",
    "build_dol_initial_claims_release_requests",
    "dol_initial_claims_coverage_from_manifest",
    "load_packaged_dol_initial_claims_archive_manifest",
    "load_packaged_dol_initial_claims_index_pages",
    "packaged_dol_initial_claims_indexes_path",
    "packaged_dol_initial_claims_manifest_path",
    "parse_dol_initial_claims_release_index",
    "replay_dol_initial_claims_archive",
]
