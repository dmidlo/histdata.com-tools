"""Occurrence-specific Census New Residential Construction archive.

The official NRC archive mixes separate text releases for starts/permits and
completions, unified PDFs, three broken archive links, a duplicated 2013
shutdown publication, and later catch-up reports.  This module retains those
facts while reconstructing initial seasonally-adjusted annual-rate levels for
building permits, housing starts, and housing completions without converting
one publication into several invented release times.
"""

from __future__ import annotations

import base64
import calendar
import hashlib
import html
import json
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
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
    UnitedStatesProgramCoverageV1,
)
from histdatacom.runtime_contracts import JSONValue

HOUSING_INDEX_ENTRY_SCHEMA_VERSION = "histdatacom.housing-index-entry.v1"
HOUSING_INDEX_SCHEMA_VERSION = "histdatacom.housing-index.v1"
HOUSING_ARTIFACT_SCHEMA_VERSION = "histdatacom.housing-artifact.v1"
HOUSING_MEASURE_SCHEMA_VERSION = "histdatacom.housing-measure.v1"
HOUSING_PUBLICATION_SCHEMA_VERSION = "histdatacom.housing-publication.v1"
HOUSING_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.housing-archive-manifest.v1"
)
HOUSING_INDEX_ENVELOPE_SCHEMA_VERSION = "histdatacom.housing-index-envelope.v1"

HOUSING_SOURCE_KEY = "us.census.new-residential-construction"
HOUSING_PROGRAM_KEY = "us.census.housing-starts-permits"
HOUSING_INDEX_URI = "https://www.census.gov/construction/nrc/data/releases.html"
HOUSING_RELEASE_SCHEDULE_URI = "https://www.census.gov/economic-indicators/"
HOUSING_FIRST_REFERENCE_PERIOD = "2000-01"
HOUSING_PREDECESSOR_REFERENCE_PERIOD = "1999-12"
HOUSING_SPLIT_LAST_REFERENCE_PERIOD = "2001-03"
HOUSING_LATEST_PACKAGED_REFERENCE_PERIOD = "2026-07"

HOUSING_BUILDING_PERMITS = "building-permits"
HOUSING_STARTS = "housing-starts"
HOUSING_COMPLETIONS = "housing-completions"
HOUSING_MEASURE_KEYS = (
    HOUSING_BUILDING_PERMITS,
    HOUSING_STARTS,
    HOUSING_COMPLETIONS,
)

HOUSING_SPLIT_STARTS_PERMITS = "starts-permits-text"
HOUSING_SPLIT_COMPLETIONS = "completions-text"
HOUSING_UNIFIED_PDF = "unified-pdf"
HOUSING_PUBLICATION_KINDS = (
    HOUSING_SPLIT_STARTS_PERMITS,
    HOUSING_SPLIT_COMPLETIONS,
    HOUSING_UNIFIED_PDF,
)

HOUSING_PREDECESSOR_URIS = (
    "https://www.census.gov/construction/nrc/txt/c20_9912.txt",
    "https://www.census.gov/construction/nrc/txt/c22_9912.txt",
)
HOUSING_MISSED_SEPARATE_PERIODS = ("2025-09", "2025-11", "2026-02")
HOUSING_SPECIAL_PRIMARY_PERIODS = (
    "2013-10",
    "2013-11",
    "2025-10",
    "2025-12",
    "2026-03",
)
HOUSING_DISRUPTION_PRIMARY_PERIODS = (
    "2013-10",
    "2013-11",
    "2018-12",
    "2019-01",
    "2019-02",
    "2025-10",
    "2025-12",
    "2026-03",
)

HOUSING_NOVEMBER_2000_LISTED_URI = (
    "https://www.census.gov/construction/nrc/txt/c22_0001.txt"
)
HOUSING_NOVEMBER_2000_ARTIFACT_URI = (
    "https://www.census.gov/construction/nrc/txt/c22_0011.txt"
)
HOUSING_APRIL_2009_LISTED_URI = (
    "fhttps://www.census.gov/construction/nrc/pdf/newresconst_200904.pdf"
)
HOUSING_APRIL_2009_ARTIFACT_URI = (
    "https://www.census.gov/construction/nrc/pdf/newresconst_200904.pdf"
)
HOUSING_AUGUST_2012_LISTED_URI = (
    "https://www.census.gov/construction/nrc/pdf/newresconst_201209.pdf"
)
HOUSING_AUGUST_2012_ARTIFACT_URI = (
    "https://www.census.gov/construction/nrc/pdf/newresconst_201208.pdf"
)

HOUSING_2013_SEPTEMBER_URI = (
    "https://www.census.gov/construction/nrc/pdf/newresconst_201309.pdf"
)
HOUSING_2013_OCTOBER_URI = (
    "https://www.census.gov/construction/nrc/pdf/newresconst_201310.pdf"
)

MAX_HOUSING_INDEX_BYTES = 4 * 1024 * 1024
MAX_HOUSING_ARTIFACT_BYTES = 32 * 1024 * 1024
MAX_HOUSING_RELEASES = 600

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_YEAR_MONTH_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")
_DATE_RE = re.compile(
    r"(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(?P<day>\d{1,2}),\s*"
    r"(?P<year>\d{4})",
    re.IGNORECASE,
)
_TIME_RE = re.compile(
    r"(?P<hour>8|10)\s*:\s*(?P<minute>00|30)\s*"
    r"(?:A\.?\s*M\.?\s*)?(?P<zone>EST|EDT|ET)",
    re.IGNORECASE,
)
_RELEASE_MARKER_RE = re.compile(
    r"\bfor\s+(?:immediate\s+)?release(?:\s+at)?\b", re.IGNORECASE
)
_NOTICE_RE = re.compile(
    r"notice of (?:revision|methodology change)|annual revision|"
    r"revised (?:estimates|data).{0,100}back to|new monthly samples|"
    r"increase in the universe",
    re.IGNORECASE | re.DOTALL,
)
_MONTH_NUMBERS = {
    name.lower(): number
    for number, name in enumerate(calendar.month_name)
    if name
}
_WORD_REPAIRS = (
    "privately",
    "owned",
    "housing",
    "building",
    "permits",
    "authorized",
    "seasonally",
    "adjusted",
    "annual",
    "starts",
    "completions",
    "revised",
    "estimate",
    "figure",
    "rate",
)

_LINK_CORRECTIONS = MappingProxyType(
    {
        ("2000-11", HOUSING_SPLIT_COMPLETIONS): (
            HOUSING_NOVEMBER_2000_LISTED_URI,
            HOUSING_NOVEMBER_2000_ARTIFACT_URI,
            "wrong-target",
        ),
        ("2009-04", HOUSING_UNIFIED_PDF): (
            HOUSING_APRIL_2009_LISTED_URI,
            HOUSING_APRIL_2009_ARTIFACT_URI,
            "malformed-scheme",
        ),
        ("2012-08", HOUSING_UNIFIED_PDF): (
            HOUSING_AUGUST_2012_LISTED_URI,
            HOUSING_AUGUST_2012_ARTIFACT_URI,
            "wrong-target",
        ),
    }
)

# These source-authored catch-up values are bound to immutable PDF hashes.  The
# ordinary parser still validates every other release.  Each row is reference
# period, measure, current level, previous reference period, revised previous.
_SPECIAL_VALUES = MappingProxyType(
    {
        "b2e78013595e75261bf82169c08d832c3523bb0d9257ce90a5ae25c60a3b12ca": (
            ("2013-09", HOUSING_BUILDING_PERMITS, 974, "2013-08", 926),
            ("2013-10", HOUSING_BUILDING_PERMITS, 1034, "2013-09", 974),
        ),
        "ce6eb310efc59e9aa60511ac678c196cb722c272bd72488a69a16761ed92fe64": (
            ("2013-09", HOUSING_STARTS, 873, "2013-08", 883),
            ("2013-09", HOUSING_COMPLETIONS, 762, "2013-08", 765),
            ("2013-10", HOUSING_STARTS, 889, "2013-09", 873),
            ("2013-10", HOUSING_COMPLETIONS, 824, "2013-09", 762),
            ("2013-11", HOUSING_BUILDING_PERMITS, 1007, "2013-10", 1039),
            ("2013-11", HOUSING_STARTS, 1091, "2013-10", 889),
            ("2013-11", HOUSING_COMPLETIONS, 823, "2013-10", 824),
        ),
        "e088e659bfb976d8a592e4e1a57bd53f50d4efb87ec9c9ef9b958137ee74a4c0": (
            ("2025-09", HOUSING_BUILDING_PERMITS, 1415, "2025-08", 1330),
            ("2025-09", HOUSING_STARTS, 1306, "2025-08", 1291),
            ("2025-09", HOUSING_COMPLETIONS, 1371, "2025-08", 1556),
            ("2025-10", HOUSING_BUILDING_PERMITS, 1412, "2025-09", 1415),
            ("2025-10", HOUSING_STARTS, 1246, "2025-09", 1306),
            ("2025-10", HOUSING_COMPLETIONS, 1386, "2025-09", 1371),
        ),
        "c9c050bd6a334c879811409c7bd43205087ce9d7dd599bd251008e7284370586": (
            ("2025-11", HOUSING_BUILDING_PERMITS, 1388, "2025-10", 1411),
            ("2025-11", HOUSING_STARTS, 1322, "2025-10", 1272),
            ("2025-11", HOUSING_COMPLETIONS, 1490, "2025-10", 1430),
            ("2025-12", HOUSING_BUILDING_PERMITS, 1448, "2025-11", 1388),
            ("2025-12", HOUSING_STARTS, 1404, "2025-11", 1322),
            ("2025-12", HOUSING_COMPLETIONS, 1525, "2025-11", 1490),
        ),
        "495ee39b847dbf992e11c0759b9f8d62104d59a75b71e6fcc0efed5d36abf0fc": (
            ("2026-02", HOUSING_BUILDING_PERMITS, 1538, "2026-01", 1386),
            ("2026-02", HOUSING_STARTS, 1356, "2026-01", 1398),
            ("2026-02", HOUSING_COMPLETIONS, 1364, "2026-01", 1455),
            ("2026-03", HOUSING_BUILDING_PERMITS, 1372, "2026-02", 1538),
            ("2026-03", HOUSING_STARTS, 1502, "2026-02", 1356),
            ("2026-03", HOUSING_COMPLETIONS, 1366, "2026-02", 1364),
        ),
    }
)
_SPECIAL_PRIMARY_HASHES = MappingProxyType(
    {
        "2013-10": "b2e78013595e75261bf82169c08d832c3523bb0d9257ce90a5ae25c60a3b12ca",
        "2013-11": "ce6eb310efc59e9aa60511ac678c196cb722c272bd72488a69a16761ed92fe64",
        "2025-10": "e088e659bfb976d8a592e4e1a57bd53f50d4efb87ec9c9ef9b958137ee74a4c0",
        "2025-12": "c9c050bd6a334c879811409c7bd43205087ce9d7dd599bd251008e7284370586",
        "2026-03": "495ee39b847dbf992e11c0759b9f8d62104d59a75b71e6fcc0efed5d36abf0fc",
    }
)


def _required_text(value: object, name: str) -> str:
    result = str(value or "").strip()
    if not result or len(result) > 4096:
        raise ValueError(f"{name} is invalid")
    return result


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name).lower()
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} is not a SHA-256 digest")
    return result


def _year_month(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _YEAR_MONTH_RE.fullmatch(result) is None:
        raise ValueError(f"{name} is not a year-month")
    return result


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        parsed = date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} is not an ISO date") from exc
    if parsed.isoformat() != result:
        raise ValueError(f"{name} is not a canonical ISO date")
    return result


def _positive_int(value: object, name: str, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= maximum
    ):
        raise ValueError(f"{name} is outside its bound")
    return value


def _https_uri(value: object, name: str) -> str:
    result = _required_text(value, name)
    parsed = urlparse(result)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError(f"{name} is not an HTTPS URI")
    return result


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _next_month(period: str) -> str:
    year, month = map(int, period.split("-"))
    month += 1
    if month == 13:
        year += 1
        month = 1
    return f"{year:04d}-{month:02d}"


def _previous_month(period: str) -> str:
    year, month = map(int, period.split("-"))
    month -= 1
    if month == 0:
        year -= 1
        month = 12
    return f"{year:04d}-{month:02d}"


def _month_end(period: str) -> str:
    year, month = map(int, period.split("-"))
    return f"{period}-{calendar.monthrange(year, month)[1]:02d}"


def _normalized_source_text(value: str) -> str:
    result = unicodedata.normalize("NFKC", value).replace("\xa0", " ")
    translation: dict[str | int, str | int | None] = {
        "‐": "-",
        "‑": "-",
        "‒": "-",
        "–": "-",
        "—": "-",
    }
    result = result.translate(str.maketrans(translation))
    for word in _WORD_REPAIRS:
        result = re.sub(
            r"\b" + r"\s*".join(map(re.escape, word)) + r"\b",
            word,
            result,
            flags=re.IGNORECASE,
        )
    result = re.sub(r"(?<=\d)\s+(?=[,\d])", "", result)
    result = re.sub(r"(?<=,)\s+(?=\d)", "", result)
    return re.sub(r"\s+", " ", result).strip()


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[dict[str, str]] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        if tag.lower() == "a":
            self.anchors.append(
                {key.lower(): value or "" for key, value in attrs}
            )


def _index_title(title: str) -> tuple[str, str] | None:
    match = re.fullmatch(
        r"(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{4})"
        r"(?:\s+(Housing Starts and Building Permits|Housing Completions|"
        r"New Residential Construction)\s+Release)?",
        html.unescape(title).strip(),
        re.IGNORECASE,
    )
    if match is None:
        return None
    period = f"{int(match.group(2)):04d}-{_MONTH_NUMBERS[match.group(1).lower()]:02d}"
    suffix = (match.group(3) or "").lower()
    kind = (
        HOUSING_SPLIT_STARTS_PERMITS
        if "starts and building permits" in suffix
        else (
            HOUSING_SPLIT_COMPLETIONS
            if "completions" in suffix
            else HOUSING_UNIFIED_PDF
        )
    )
    return period, kind


@dataclass(frozen=True, slots=True)
class CensusHousingReleaseIndexEntryV1:
    """One archive-listed artifact and any bounded URI correction."""

    reference_period: str
    publication_kind: str
    listed_uri: str
    artifact_uri: str
    correction_kind: str
    entry_id: str = ""
    schema_version: str = HOUSING_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != HOUSING_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported housing index-entry schema")
        period = _year_month(self.reference_period, "reference_period")
        kind = _required_text(self.publication_kind, "publication_kind")
        if kind not in HOUSING_PUBLICATION_KINDS:
            raise ValueError("housing publication kind differs")
        listed = _required_text(self.listed_uri, "listed_uri")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        correction = _required_text(self.correction_kind, "correction_kind")
        expected = _LINK_CORRECTIONS.get((period, kind))
        if expected is None:
            if listed != artifact or correction != "none":
                raise ValueError("housing link correction differs")
        elif (listed, artifact, correction) != expected:
            raise ValueError("housing link correction differs")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "publication_kind", kind)
        object.__setattr__(self, "listed_uri", listed)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "correction_kind", correction)
        expected_id = _stable_id("housing-index-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected_id:
            raise ValueError("housing index-entry identity differs")
        object.__setattr__(self, "entry_id", expected_id)

    @property
    def link_corrected(self) -> bool:
        return self.correction_kind != "none"

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "publication_kind": self.publication_kind,
            "listed_uri": self.listed_uri,
            "artifact_uri": self.artifact_uri,
            "correction_kind": self.correction_kind,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusHousingReleaseIndexEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            publication_kind=str(data.get("publication_kind", "")),
            listed_uri=str(data.get("listed_uri", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            correction_kind=str(data.get("correction_kind", "")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusHousingReleaseIndexV1:
    """Hash-bound enumeration of every target artifact link."""

    source_uri: str
    content_sha256: str
    content_length: int
    as_of_date: str
    releases: tuple[CensusHousingReleaseIndexEntryV1, ...]
    index_id: str = ""
    schema_version: str = HOUSING_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != HOUSING_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported housing release-index schema")
        if _https_uri(self.source_uri, "source_uri") != HOUSING_INDEX_URI:
            raise ValueError("housing index URI differs")
        sha = _sha256(self.content_sha256, "content_sha256")
        _positive_int(
            self.content_length, "content_length", MAX_HOUSING_INDEX_BYTES
        )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        releases = tuple(self.releases)
        if (
            not releases
            or len(releases) > MAX_HOUSING_RELEASES
            or any(
                not isinstance(item, CensusHousingReleaseIndexEntryV1)
                for item in releases
            )
        ):
            raise TypeError("housing index releases are invalid")
        keys = tuple(
            (item.reference_period, item.publication_kind) for item in releases
        )
        order = {
            HOUSING_SPLIT_STARTS_PERMITS: 0,
            HOUSING_SPLIT_COMPLETIONS: 1,
            HOUSING_UNIFIED_PDF: 0,
        }
        expected_order = tuple(
            sorted(keys, key=lambda item: (item[0], order[item[1]]))
        )
        if keys != expected_order or len(set(keys)) != len(keys):
            raise ValueError("housing index entries are not uniquely ordered")
        last = releases[-1].reference_period
        if last < HOUSING_LATEST_PACKAGED_REFERENCE_PERIOD:
            raise ValueError("housing index ends before the qualified corpus")
        expected_keys: list[tuple[str, str]] = []
        period = HOUSING_FIRST_REFERENCE_PERIOD
        while period <= last:
            if period <= HOUSING_SPLIT_LAST_REFERENCE_PERIOD:
                expected_keys.extend(
                    (
                        (period, HOUSING_SPLIT_STARTS_PERMITS),
                        (period, HOUSING_SPLIT_COMPLETIONS),
                    )
                )
            elif period not in HOUSING_MISSED_SEPARATE_PERIODS:
                expected_keys.append((period, HOUSING_UNIFIED_PDF))
            period = _next_month(period)
        if keys != tuple(expected_keys):
            raise ValueError("housing index has an unexplained period gap")
        if sum(item.link_corrected for item in releases) != 3:
            raise ValueError("housing index correction count differs")
        if len({item.artifact_uri for item in releases}) != len(releases):
            raise ValueError("housing index repeats a corrected artifact URI")
        object.__setattr__(self, "content_sha256", sha)
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "releases", releases)
        expected_id = _stable_id("housing-index", self.identity_payload())
        if self.index_id and self.index_id != expected_id:
            raise ValueError("housing index identity differs")
        object.__setattr__(self, "index_id", expected_id)

    @property
    def by_key(
        self,
    ) -> Mapping[tuple[str, str], CensusHousingReleaseIndexEntryV1]:
        return MappingProxyType(
            {
                (item.reference_period, item.publication_kind): item
                for item in self.releases
            }
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_uri": self.source_uri,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "as_of_date": self.as_of_date,
            "releases": [item.to_dict() for item in self.releases],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CensusHousingReleaseIndexV1:
        return cls(
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            as_of_date=str(data.get("as_of_date", "")),
            releases=tuple(
                CensusHousingReleaseIndexEntryV1.from_dict(
                    _mapping(item, "housing index entry")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusHousingArtifactV1:
    """Compact hash evidence for one retained official response."""

    artifact_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    artifact_id: str = ""
    schema_version: str = HOUSING_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != HOUSING_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported housing artifact schema")
        uri = _https_uri(self.artifact_uri, "artifact_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        if source_format not in {
            OfficialSourceFormat.HTML,
            OfficialSourceFormat.PDF,
            OfficialSourceFormat.TEXT,
        }:
            raise ValueError("housing artifact format differs")
        sha = _sha256(self.content_sha256, "content_sha256")
        limit = (
            MAX_HOUSING_INDEX_BYTES
            if source_format is OfficialSourceFormat.HTML
            else MAX_HOUSING_ARTIFACT_BYTES
        )
        _positive_int(self.content_length, "content_length", limit)
        object.__setattr__(self, "artifact_uri", uri)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "content_sha256", sha)
        expected = _stable_id("housing-artifact", self.identity_payload())
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("housing artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "artifact_uri": self.artifact_uri,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CensusHousingArtifactV1:
        return cls(
            artifact_uri=str(data.get("artifact_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _level_thousands(
    lexical: object, numeric: object, name: str
) -> tuple[str, int]:
    text = _required_text(lexical, f"{name}_lexical")
    if re.fullmatch(r"\d{1,3}(?:,\d{3})+", text) is None:
        raise ValueError(f"{name}_lexical is not a source unit count")
    units = int(text.replace(",", ""))
    if units % 1000 or not 1 <= units // 1000 <= 10_000:
        raise ValueError(f"{name}_lexical is not whole thousands")
    if not isinstance(numeric, int) or isinstance(numeric, bool):
        raise TypeError(f"{name}_thousands must be an integer")
    if numeric != units // 1000:
        raise ValueError(f"{name} lexical and numeric levels differ")
    return text, numeric


@dataclass(frozen=True, slots=True)
class CensusHousingMeasureV1:
    """One newly published level and its prior-vintage lineage."""

    reference_period: str
    measure_key: str
    actual_thousands: int
    actual_lexical: str
    previous_reference_period: str
    revised_previous_thousands: int
    revised_previous_lexical: str
    previous_as_known_reference_period: str | None
    previous_as_known_thousands: int | None
    previous_as_known_lexical: str | None
    previous_artifact_uri: str | None
    revision_comparable: bool
    comparison_basis: str
    measure_id: str = ""
    schema_version: str = HOUSING_MEASURE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != HOUSING_MEASURE_SCHEMA_VERSION:
            raise ValueError("unsupported housing measure schema")
        period = _year_month(self.reference_period, "reference_period")
        key = _required_text(self.measure_key, "measure_key")
        if key not in HOUSING_MEASURE_KEYS:
            raise ValueError("housing measure key differs")
        actual_text, actual = _level_thousands(
            self.actual_lexical, self.actual_thousands, "actual"
        )
        previous_period = _year_month(
            self.previous_reference_period, "previous_reference_period"
        )
        if previous_period != _previous_month(period):
            raise ValueError("housing revised previous period differs")
        revised_text, revised = _level_thousands(
            self.revised_previous_lexical,
            self.revised_previous_thousands,
            "revised_previous",
        )
        known_period = _optional_text(self.previous_as_known_reference_period)
        known_text = _optional_text(self.previous_as_known_lexical)
        known_uri = _optional_text(self.previous_artifact_uri)
        if known_period is None:
            if any(
                item is not None
                for item in (
                    self.previous_as_known_thousands,
                    known_text,
                    known_uri,
                )
            ):
                raise ValueError(
                    "housing previous-as-known evidence is partial"
                )
        else:
            known_period = _year_month(
                known_period, "previous_as_known_reference_period"
            )
            if known_text is None or known_uri is None:
                raise ValueError(
                    "housing previous-as-known evidence is partial"
                )
            known_text, _ = _level_thousands(
                known_text,
                self.previous_as_known_thousands,
                "previous_as_known",
            )
            known_uri = _https_uri(known_uri, "previous_artifact_uri")
        if not isinstance(self.revision_comparable, bool):
            raise TypeError("revision_comparable must be boolean")
        comparable = known_period == previous_period
        if self.revision_comparable != comparable:
            raise ValueError("housing comparison state differs")
        basis = _required_text(self.comparison_basis, "comparison_basis")
        expected_basis = (
            "prior-publication" if comparable else "same-publication-catch-up"
        )
        if basis != expected_basis:
            raise ValueError("housing comparison basis differs")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "measure_key", key)
        object.__setattr__(self, "actual_thousands", actual)
        object.__setattr__(self, "actual_lexical", actual_text)
        object.__setattr__(self, "previous_reference_period", previous_period)
        object.__setattr__(self, "revised_previous_thousands", revised)
        object.__setattr__(self, "revised_previous_lexical", revised_text)
        object.__setattr__(
            self, "previous_as_known_reference_period", known_period
        )
        object.__setattr__(self, "previous_as_known_lexical", known_text)
        object.__setattr__(self, "previous_artifact_uri", known_uri)
        object.__setattr__(self, "comparison_basis", basis)
        expected = _stable_id("housing-measure", self.identity_payload())
        if self.measure_id and self.measure_id != expected:
            raise ValueError("housing measure identity differs")
        object.__setattr__(self, "measure_id", expected)

    @property
    def previous_was_revised(self) -> bool:
        return bool(
            self.revision_comparable
            and self.previous_as_known_thousands
            != self.revised_previous_thousands
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "measure_key": self.measure_key,
            "actual_thousands": self.actual_thousands,
            "actual_lexical": self.actual_lexical,
            "previous_reference_period": self.previous_reference_period,
            "revised_previous_thousands": self.revised_previous_thousands,
            "revised_previous_lexical": self.revised_previous_lexical,
            "previous_as_known_reference_period": (
                self.previous_as_known_reference_period
            ),
            "previous_as_known_thousands": self.previous_as_known_thousands,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "previous_artifact_uri": self.previous_artifact_uri,
            "revision_comparable": self.revision_comparable,
            "comparison_basis": self.comparison_basis,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "measure_id": self.measure_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CensusHousingMeasureV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            measure_key=str(data.get("measure_key", "")),
            actual_thousands=cast(int, data.get("actual_thousands")),
            actual_lexical=str(data.get("actual_lexical", "")),
            previous_reference_period=str(
                data.get("previous_reference_period", "")
            ),
            revised_previous_thousands=cast(
                int, data.get("revised_previous_thousands")
            ),
            revised_previous_lexical=str(
                data.get("revised_previous_lexical", "")
            ),
            previous_as_known_reference_period=cast(
                str | None, data.get("previous_as_known_reference_period")
            ),
            previous_as_known_thousands=cast(
                int | None, data.get("previous_as_known_thousands")
            ),
            previous_as_known_lexical=cast(
                str | None, data.get("previous_as_known_lexical")
            ),
            previous_artifact_uri=cast(
                str | None, data.get("previous_artifact_uri")
            ),
            revision_comparable=cast(bool, data.get("revision_comparable")),
            comparison_basis=str(data.get("comparison_basis", "")),
            measure_id=str(data.get("measure_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusHousingPublicationV1:
    """One real publication time, including all catch-up values and aliases."""

    index_entry_ids: tuple[str, ...]
    primary_reference_period: str
    release_date: str
    released_lexical: str
    released_at_ns: int
    reported_zone: str
    zone_consistent: bool
    artifacts: tuple[CensusHousingArtifactV1, ...]
    parser_era: str
    source_header_lexical: str
    historical_revision_notice: bool
    catch_up_publication: bool
    federal_disruption_notice: bool
    measures: tuple[CensusHousingMeasureV1, ...]
    publication_id: str = ""
    schema_version: str = HOUSING_PUBLICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != HOUSING_PUBLICATION_SCHEMA_VERSION:
            raise ValueError("unsupported housing publication schema")
        entry_ids = tuple(
            _required_text(item, "index_entry_ids")
            for item in self.index_entry_ids
        )
        if not 1 <= len(entry_ids) <= 2 or len(set(entry_ids)) != len(
            entry_ids
        ):
            raise ValueError("housing publication index identities differ")
        if any(
            not item.startswith("housing-index-entry:sha256:")
            for item in entry_ids
        ):
            raise ValueError("housing publication index identity is invalid")
        primary = _year_month(
            self.primary_reference_period, "primary_reference_period"
        )
        release = _iso_date(self.release_date, "release_date")
        released = _required_text(self.released_lexical, "released_lexical")
        if (
            re.fullmatch(rf"{re.escape(release)}T(?:08:30|10:00):00", released)
            is None
        ):
            raise ValueError("housing release lexical timestamp differs")
        if not isinstance(self.released_at_ns, int) or self.released_at_ns <= 0:
            raise ValueError("housing release timestamp differs")
        zone = _required_text(self.reported_zone, "reported_zone").upper()
        if zone not in {"EST", "EDT"} or not isinstance(
            self.zone_consistent, bool
        ):
            raise ValueError("housing reported zone differs")
        expected_ns = int(
            datetime.fromisoformat(released)
            .replace(tzinfo=ZoneInfo("America/New_York"))
            .timestamp()
            * 1_000_000_000
        )
        if self.released_at_ns != expected_ns:
            raise ValueError("housing normalized timestamp differs")
        expected_zone = (
            datetime.fromisoformat(released)
            .replace(tzinfo=ZoneInfo("America/New_York"))
            .tzname()
        )
        if self.zone_consistent != (zone == expected_zone):
            raise ValueError("housing source zone consistency differs")
        artifacts = tuple(self.artifacts)
        if (
            not artifacts
            or len(artifacts) > 2
            or any(
                not isinstance(item, CensusHousingArtifactV1)
                for item in artifacts
            )
            or tuple(item.artifact_uri for item in artifacts)
            != tuple(sorted({item.artifact_uri for item in artifacts}))
        ):
            raise ValueError("housing publication artifacts differ")
        era = _required_text(self.parser_era, "parser_era")
        if era not in {
            "legacy-split-text",
            "native-pdf-layout",
            "native-pdf-standard",
            "hash-bound-catch-up-pdf",
        }:
            raise ValueError("housing parser era differs")
        if era == "legacy-split-text" and any(
            item.source_format is not OfficialSourceFormat.TEXT
            for item in artifacts
        ):
            raise ValueError("housing text era artifact differs")
        if era != "legacy-split-text" and any(
            item.source_format is not OfficialSourceFormat.PDF
            for item in artifacts
        ):
            raise ValueError("housing PDF era artifact differs")
        header = _required_text(
            self.source_header_lexical, "source_header_lexical"
        )
        if not isinstance(
            self.historical_revision_notice, bool
        ) or not isinstance(self.catch_up_publication, bool):
            raise TypeError("housing publication flags must be boolean")
        if not isinstance(self.federal_disruption_notice, bool):
            raise TypeError("housing disruption flag must be boolean")
        values = tuple(self.measures)
        keys = tuple(
            (item.reference_period, item.measure_key) for item in values
        )
        if (
            not values
            or len(values) > 12
            or any(
                not isinstance(item, CensusHousingMeasureV1) for item in values
            )
            or keys != tuple(sorted(keys))
            or len(set(keys)) != len(keys)
        ):
            raise ValueError("housing publication measures differ")
        special = primary in HOUSING_SPECIAL_PRIMARY_PERIODS
        if self.catch_up_publication != special:
            raise ValueError("housing catch-up publication state differs")
        if self.federal_disruption_notice != (
            primary in HOUSING_DISRUPTION_PRIMARY_PERIODS
        ):
            raise ValueError("housing disruption publication state differs")
        expected_counts = {
            "2013-10": 2,
            "2013-11": 7,
            "2025-10": 6,
            "2025-12": 6,
            "2026-03": 6,
        }
        if special and len(values) != expected_counts[primary]:
            raise ValueError("housing catch-up value inventory differs")
        expected_special_hash = _SPECIAL_PRIMARY_HASHES.get(primary)
        artifact_hashes = {item.content_sha256 for item in artifacts}
        if special:
            if expected_special_hash not in artifact_hashes or len(
                artifacts
            ) != (2 if primary == "2013-10" else 1):
                raise ValueError("housing catch-up artifact binding differs")
            expected_rows = _SPECIAL_VALUES[expected_special_hash]
            observed_rows = tuple(
                (
                    item.reference_period,
                    item.measure_key,
                    item.actual_thousands,
                    item.previous_reference_period,
                    item.revised_previous_thousands,
                )
                for item in values
            )
            if observed_rows != tuple(
                sorted(expected_rows, key=lambda item: (item[0], item[1]))
            ):
                raise ValueError("housing catch-up values differ")
        elif artifact_hashes & set(_SPECIAL_VALUES):
            raise ValueError("housing catch-up artifact is misclassified")
        object.__setattr__(self, "index_entry_ids", entry_ids)
        object.__setattr__(self, "primary_reference_period", primary)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "released_lexical", released)
        object.__setattr__(self, "reported_zone", zone)
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "parser_era", era)
        object.__setattr__(self, "source_header_lexical", header)
        object.__setattr__(self, "measures", values)
        expected = _stable_id("housing-publication", self.identity_payload())
        if self.publication_id and self.publication_id != expected:
            raise ValueError("housing publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    @property
    def by_reference_measure(
        self,
    ) -> Mapping[tuple[str, str], CensusHousingMeasureV1]:
        return MappingProxyType(
            {
                (item.reference_period, item.measure_key): item
                for item in self.measures
            }
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "index_entry_ids": list(self.index_entry_ids),
            "primary_reference_period": self.primary_reference_period,
            "release_date": self.release_date,
            "released_lexical": self.released_lexical,
            "released_at_ns": self.released_at_ns,
            "reported_zone": self.reported_zone,
            "zone_consistent": self.zone_consistent,
            "artifacts": [item.to_dict() for item in self.artifacts],
            "parser_era": self.parser_era,
            "source_header_lexical": self.source_header_lexical,
            "historical_revision_notice": self.historical_revision_notice,
            "catch_up_publication": self.catch_up_publication,
            "federal_disruption_notice": self.federal_disruption_notice,
            "measures": [item.to_dict() for item in self.measures],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "publication_id": self.publication_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CensusHousingPublicationV1:
        return cls(
            index_entry_ids=tuple(
                str(item)
                for item in _sequence(
                    data.get("index_entry_ids"), "index_entry_ids"
                )
            ),
            primary_reference_period=str(
                data.get("primary_reference_period", "")
            ),
            release_date=str(data.get("release_date", "")),
            released_lexical=str(data.get("released_lexical", "")),
            released_at_ns=cast(int, data.get("released_at_ns")),
            reported_zone=str(data.get("reported_zone", "")),
            zone_consistent=cast(bool, data.get("zone_consistent")),
            artifacts=tuple(
                CensusHousingArtifactV1.from_dict(
                    _mapping(item, "housing publication artifact")
                )
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            parser_era=str(data.get("parser_era", "")),
            source_header_lexical=str(data.get("source_header_lexical", "")),
            historical_revision_notice=cast(
                bool, data.get("historical_revision_notice")
            ),
            catch_up_publication=cast(bool, data.get("catch_up_publication")),
            federal_disruption_notice=cast(
                bool, data.get("federal_disruption_notice")
            ),
            measures=tuple(
                CensusHousingMeasureV1.from_dict(
                    _mapping(item, "housing publication measure")
                )
                for item in _sequence(data.get("measures"), "measures")
            ),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusHousingArchiveManifestV1:
    """Compact replay receipt for the complete NRC publication corpus."""

    registry_id: str
    profile_id: str
    release_index: CensusHousingReleaseIndexV1
    predecessors: tuple[CensusHousingArtifactV1, ...]
    publications: tuple[CensusHousingPublicationV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    measure_occurrence_count: int
    previous_as_known_measure_count: int
    comparable_measure_count: int
    changed_revision_count: int
    corrected_link_count: int
    split_text_publication_count: int
    layout_pdf_publication_count: int
    standard_pdf_fallback_count: int
    hash_bound_special_publication_count: int
    catch_up_publication_count: int
    disruption_publication_count: int
    historical_revision_notice_count: int
    ten_am_publication_count: int
    exact_minute_count: int
    zone_mismatch_count: int
    manifest_id: str = ""
    schema_version: str = HOUSING_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != HOUSING_ARCHIVE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported housing archive-manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("housing registry identity differs")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("housing profile identity differs")
        if not isinstance(self.release_index, CensusHousingReleaseIndexV1):
            raise TypeError("housing manifest requires a v1 release index")
        predecessors = tuple(self.predecessors)
        if (
            len(predecessors) != 2
            or any(
                not isinstance(item, CensusHousingArtifactV1)
                for item in predecessors
            )
            or tuple(item.artifact_uri for item in predecessors)
            != HOUSING_PREDECESSOR_URIS
            or any(
                item.source_format is not OfficialSourceFormat.TEXT
                for item in predecessors
            )
        ):
            raise ValueError("housing predecessor evidence differs")
        publications = tuple(self.publications)
        chronology = tuple(item.released_at_ns for item in publications)
        if (
            len(publications) != len(self.release_index.releases) - 1
            or any(
                not isinstance(item, CensusHousingPublicationV1)
                for item in publications
            )
            or chronology != tuple(sorted(set(chronology)))
        ):
            inversions = tuple(
                (
                    publications[index - 1].primary_reference_period,
                    publications[index].primary_reference_period,
                )
                for index in range(1, len(publications))
                if chronology[index] <= chronology[index - 1]
            )
            raise ValueError(
                f"housing publication chronology differs: {inversions}"
            )
        indexed_ids = {item.entry_id for item in self.release_index.releases}
        publication_ids = [
            entry_id
            for publication in publications
            for entry_id in publication.index_entry_ids
        ]
        if set(publication_ids) != indexed_ids or len(publication_ids) != len(
            indexed_ids
        ):
            raise ValueError(
                "housing manifest does not cover every index entry"
            )
        indexed_uris = {
            item.artifact_uri for item in self.release_index.releases
        }
        publication_uris = {
            artifact.artifact_uri
            for publication in publications
            for artifact in publication.artifacts
        }
        if publication_uris != indexed_uris:
            raise ValueError("housing manifest artifact inventory differs")
        entries_by_id = {
            item.entry_id: item for item in self.release_index.releases
        }
        known: dict[tuple[str, str], tuple[int, str]] = {}
        predecessor_by_measure = {
            HOUSING_BUILDING_PERMITS: HOUSING_PREDECESSOR_URIS[0],
            HOUSING_STARTS: HOUSING_PREDECESSOR_URIS[0],
            HOUSING_COMPLETIONS: HOUSING_PREDECESSOR_URIS[1],
        }
        for publication in publications:
            primary_entry = entries_by_id[publication.index_entry_ids[-1]]
            if (
                primary_entry.reference_period
                != publication.primary_reference_period
                or primary_entry.artifact_uri
                not in {item.artifact_uri for item in publication.artifacts}
            ):
                raise ValueError("housing publication index binding differs")
            prior_known = dict(known)
            for measure in publication.measures:
                expected_previous = prior_known.get(
                    (measure.previous_reference_period, measure.measure_key)
                )
                if expected_previous is None:
                    if (
                        measure.reference_period
                        == HOUSING_FIRST_REFERENCE_PERIOD
                    ):
                        if (
                            not measure.revision_comparable
                            or measure.previous_artifact_uri
                            != predecessor_by_measure[measure.measure_key]
                        ):
                            raise ValueError(
                                "housing predecessor lineage differs"
                            )
                    elif (
                        measure.revision_comparable
                        or measure.previous_as_known_thousands is not None
                        or measure.previous_artifact_uri is not None
                    ):
                        raise ValueError("housing catch-up lineage differs")
                elif (
                    not measure.revision_comparable
                    or measure.previous_as_known_thousands
                    != expected_previous[0]
                    or measure.previous_artifact_uri != expected_previous[1]
                ):
                    raise ValueError(
                        "housing previous-as-known lineage differs"
                    )
            for measure in publication.measures:
                known[(measure.reference_period, measure.measure_key)] = (
                    measure.actual_thousands,
                    primary_entry.artifact_uri,
                )
        values = tuple(
            measure
            for publication in publications
            for measure in publication.measures
        )
        by_key = {
            (item.reference_period, item.measure_key): item for item in values
        }
        if len(by_key) != len(values):
            raise ValueError("housing manifest repeats a measure occurrence")
        expected_keys: set[tuple[str, str]] = set()
        period = HOUSING_FIRST_REFERENCE_PERIOD
        last = self.release_index.releases[-1].reference_period
        while period <= last:
            expected_keys.update((period, key) for key in HOUSING_MEASURE_KEYS)
            period = _next_month(period)
        if set(by_key) != expected_keys:
            raise ValueError("housing monthly measure coverage differs")
        all_artifacts = (
            CensusHousingArtifactV1(
                artifact_uri=self.release_index.source_uri,
                source_format=OfficialSourceFormat.HTML,
                content_sha256=self.release_index.content_sha256,
                content_length=self.release_index.content_length,
            ),
            *predecessors,
            *(
                artifact
                for publication in publications
                for artifact in publication.artifacts
            ),
        )
        observed = {
            "raw_artifact_count": len(all_artifacts),
            "unique_content_sha256_count": len(
                {item.content_sha256 for item in all_artifacts}
            ),
            "total_content_bytes": sum(
                item.content_length for item in all_artifacts
            ),
            "measure_occurrence_count": len(values),
            "previous_as_known_measure_count": sum(
                item.previous_as_known_thousands is not None for item in values
            ),
            "comparable_measure_count": sum(
                item.revision_comparable for item in values
            ),
            "changed_revision_count": sum(
                item.previous_was_revised for item in values
            ),
            "corrected_link_count": sum(
                item.link_corrected for item in self.release_index.releases
            ),
            "split_text_publication_count": sum(
                item.parser_era == "legacy-split-text" for item in publications
            ),
            "layout_pdf_publication_count": sum(
                item.parser_era == "native-pdf-layout" for item in publications
            ),
            "standard_pdf_fallback_count": sum(
                item.parser_era == "native-pdf-standard"
                for item in publications
            ),
            "hash_bound_special_publication_count": sum(
                item.parser_era == "hash-bound-catch-up-pdf"
                for item in publications
            ),
            "catch_up_publication_count": sum(
                item.catch_up_publication for item in publications
            ),
            "disruption_publication_count": sum(
                item.federal_disruption_notice for item in publications
            ),
            "historical_revision_notice_count": sum(
                item.historical_revision_notice for item in publications
            ),
            "ten_am_publication_count": sum(
                "T10:00:00" in item.released_lexical for item in publications
            ),
            "exact_minute_count": len(publications),
            "zone_mismatch_count": sum(
                not item.zone_consistent for item in publications
            ),
        }
        for name, expected_count in observed.items():
            actual = getattr(self, name)
            if (
                not isinstance(actual, int)
                or isinstance(actual, bool)
                or actual != expected_count
            ):
                raise ValueError(f"housing manifest {name} differs")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "predecessors", predecessors)
        object.__setattr__(self, "publications", publications)
        expected_id = _stable_id(
            "housing-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected_id:
            raise ValueError("housing archive-manifest identity differs")
        object.__setattr__(self, "manifest_id", expected_id)

    @property
    def by_primary_period(
        self,
    ) -> Mapping[str, tuple[CensusHousingPublicationV1, ...]]:
        grouped: dict[str, list[CensusHousingPublicationV1]] = {}
        for publication in self.publications:
            grouped.setdefault(publication.primary_reference_period, []).append(
                publication
            )
        return MappingProxyType(
            {period: tuple(items) for period, items in grouped.items()}
        )

    @property
    def by_reference_measure(
        self,
    ) -> Mapping[tuple[str, str], CensusHousingMeasureV1]:
        return MappingProxyType(
            {
                (measure.reference_period, measure.measure_key): measure
                for publication in self.publications
                for measure in publication.measures
            }
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "predecessors": [item.to_dict() for item in self.predecessors],
            "publications": [item.to_dict() for item in self.publications],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "measure_occurrence_count": self.measure_occurrence_count,
            "previous_as_known_measure_count": self.previous_as_known_measure_count,
            "comparable_measure_count": self.comparable_measure_count,
            "changed_revision_count": self.changed_revision_count,
            "corrected_link_count": self.corrected_link_count,
            "split_text_publication_count": self.split_text_publication_count,
            "layout_pdf_publication_count": self.layout_pdf_publication_count,
            "standard_pdf_fallback_count": self.standard_pdf_fallback_count,
            "hash_bound_special_publication_count": (
                self.hash_bound_special_publication_count
            ),
            "catch_up_publication_count": self.catch_up_publication_count,
            "disruption_publication_count": self.disruption_publication_count,
            "historical_revision_notice_count": (
                self.historical_revision_notice_count
            ),
            "ten_am_publication_count": self.ten_am_publication_count,
            "exact_minute_count": self.exact_minute_count,
            "zone_mismatch_count": self.zone_mismatch_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusHousingArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=CensusHousingReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "housing release index")
            ),
            predecessors=tuple(
                CensusHousingArtifactV1.from_dict(
                    _mapping(item, "housing predecessor")
                )
                for item in _sequence(data.get("predecessors"), "predecessors")
            ),
            publications=tuple(
                CensusHousingPublicationV1.from_dict(
                    _mapping(item, "housing publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            measure_occurrence_count=cast(
                int, data.get("measure_occurrence_count")
            ),
            previous_as_known_measure_count=cast(
                int, data.get("previous_as_known_measure_count")
            ),
            comparable_measure_count=cast(
                int, data.get("comparable_measure_count")
            ),
            changed_revision_count=cast(
                int, data.get("changed_revision_count")
            ),
            corrected_link_count=cast(int, data.get("corrected_link_count")),
            split_text_publication_count=cast(
                int, data.get("split_text_publication_count")
            ),
            layout_pdf_publication_count=cast(
                int, data.get("layout_pdf_publication_count")
            ),
            standard_pdf_fallback_count=cast(
                int, data.get("standard_pdf_fallback_count")
            ),
            hash_bound_special_publication_count=cast(
                int, data.get("hash_bound_special_publication_count")
            ),
            catch_up_publication_count=cast(
                int, data.get("catch_up_publication_count")
            ),
            disruption_publication_count=cast(
                int, data.get("disruption_publication_count")
            ),
            historical_revision_notice_count=cast(
                int, data.get("historical_revision_notice_count")
            ),
            ten_am_publication_count=cast(
                int, data.get("ten_am_publication_count")
            ),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            zone_mismatch_count=cast(int, data.get("zone_mismatch_count")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> CensusHousingArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "housing archive manifest is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload, "housing archive manifest"))


def build_census_housing_index_request(
    registry: OfficialSourceRegistryV1, *, as_of_date: str
) -> OfficialSourceRequestV1:
    """Plan the exact official NRC archive-index request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("housing index request requires a v1 registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(HOUSING_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=HOUSING_INDEX_URI,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=US_BACKFILL_START_DATE,
        window_end=as_of,
        page_number=1,
    )


def parse_census_housing_release_index(
    snapshot: OfficialRawSnapshotV1, *, as_of_date: str
) -> CensusHousingReleaseIndexV1:
    """Parse and close every target link, including three source defects."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("housing index parser requires a v1 snapshot")
    as_of = _iso_date(as_of_date, "as_of_date")
    if (
        snapshot.request.source_key != HOUSING_SOURCE_KEY
        or snapshot.request.uri != HOUSING_INDEX_URI
        or snapshot.request.source_format is not OfficialSourceFormat.HTML
        or snapshot.request.window_start != US_BACKFILL_START_DATE
        or snapshot.request.window_end != as_of
        or snapshot.status_code != 200
        or len(snapshot.content) > MAX_HOUSING_INDEX_BYTES
    ):
        raise ValueError("housing index snapshot differs")
    try:
        source_text = snapshot.content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("housing index is not UTF-8") from exc
    parser = _AnchorParser()
    parser.feed(source_text)
    observed: dict[tuple[str, str], str] = {}
    for anchor in parser.anchors:
        parsed = _index_title(anchor.get("title", ""))
        if parsed is None:
            continue
        period, kind = parsed
        if period < HOUSING_FIRST_REFERENCE_PERIOD:
            continue
        if period <= HOUSING_SPLIT_LAST_REFERENCE_PERIOD:
            if kind == HOUSING_UNIFIED_PDF:
                continue
        elif kind != HOUSING_UNIFIED_PDF:
            continue
        listed = html.unescape(anchor.get("href", "")).strip()
        if not listed:
            raise ValueError("housing index target has no href")
        prior = observed.setdefault((period, kind), listed)
        if prior != listed:
            raise ValueError("housing index repeats a title with another URI")
    if not observed:
        raise ValueError("housing index contains no target releases")
    kind_order = {
        HOUSING_SPLIT_STARTS_PERMITS: 0,
        HOUSING_SPLIT_COMPLETIONS: 1,
        HOUSING_UNIFIED_PDF: 0,
    }
    entries: list[CensusHousingReleaseIndexEntryV1] = []
    for (period, kind), listed in sorted(
        observed.items(), key=lambda item: (item[0][0], kind_order[item[0][1]])
    ):
        correction = _LINK_CORRECTIONS.get((period, kind))
        artifact = listed if correction is None else correction[1]
        correction_kind = "none" if correction is None else correction[2]
        entries.append(
            CensusHousingReleaseIndexEntryV1(
                reference_period=period,
                publication_kind=kind,
                listed_uri=listed,
                artifact_uri=artifact,
                correction_kind=correction_kind,
            )
        )
    return CensusHousingReleaseIndexV1(
        source_uri=HOUSING_INDEX_URI,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        as_of_date=as_of,
        releases=tuple(entries),
    )


def _release_request(
    registry: OfficialSourceRegistryV1,
    *,
    uri: str,
    period: str,
    source_format: OfficialSourceFormat,
) -> OfficialSourceRequestV1:
    source = registry.source(HOUSING_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=f"{period}-01",
        window_end=_month_end(period),
    )


def build_census_housing_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: CensusHousingReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan both predecessors and all 331 target artifact requests."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("housing requests require a v1 registry")
    if not isinstance(release_index, CensusHousingReleaseIndexV1):
        raise TypeError("housing requests require a v1 index")
    requests = [
        *(
            _release_request(
                registry,
                uri=uri,
                period=HOUSING_PREDECESSOR_REFERENCE_PERIOD,
                source_format=OfficialSourceFormat.TEXT,
            )
            for uri in HOUSING_PREDECESSOR_URIS
        ),
        *(
            _release_request(
                registry,
                uri=item.artifact_uri,
                period=item.reference_period,
                source_format=(
                    OfficialSourceFormat.TEXT
                    if item.publication_kind
                    in {
                        HOUSING_SPLIT_STARTS_PERMITS,
                        HOUSING_SPLIT_COMPLETIONS,
                    }
                    else OfficialSourceFormat.PDF
                ),
            )
            for item in release_index.releases
        ),
    ]
    if len({item.uri for item in requests}) != len(requests):
        raise ValueError("housing request plan repeats an artifact URI")
    return tuple(requests)


def _validate_release_snapshot(
    snapshot: OfficialRawSnapshotV1,
    *,
    uri: str,
    source_format: OfficialSourceFormat,
) -> None:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("housing release parser requires a v1 snapshot")
    signature_ok = (
        snapshot.content.startswith(b"%PDF-")
        if source_format is OfficialSourceFormat.PDF
        else not snapshot.content.startswith(b"%PDF-")
    )
    if (
        snapshot.request.source_key != HOUSING_SOURCE_KEY
        or snapshot.request.uri != uri
        or snapshot.request.source_format is not source_format
        or snapshot.status_code != 200
        or not signature_ok
        or len(snapshot.content) > MAX_HOUSING_ARTIFACT_BYTES
    ):
        raise ValueError("housing release snapshot differs")


def _decode_text(content: bytes) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("housing text artifact cannot be decoded")


def _source_text_candidates(
    snapshot: OfficialRawSnapshotV1,
) -> tuple[str, ...]:
    if snapshot.request.source_format is OfficialSourceFormat.TEXT:
        return (_normalized_source_text(_decode_text(snapshot.content)),)
    try:
        reader = PdfReader(BytesIO(snapshot.content), strict=False)
        pages = reader.pages[:2]
        layout = _normalized_source_text(
            "\n".join(
                page.extract_text(extraction_mode="layout") or ""
                for page in pages
            )
        )
        standard = _normalized_source_text(
            "\n".join(page.extract_text() or "" for page in pages)
        )
    except Exception as exc:
        raise ValueError("housing PDF artifact is not readable") from exc
    candidates = tuple(dict.fromkeys((layout, standard)))
    if any(not item for item in candidates):
        raise ValueError("housing PDF has no extractable first-page text")
    return candidates


def _release_metadata(
    candidates: Sequence[str],
) -> tuple[str, str, int, str, bool, str]:
    # Prefer standard extraction when both modes preserve an explicit release
    # marker, while allowing layout mode to recover overprinted legacy PDFs.
    ordered = list(reversed(candidates))
    ordered.sort(
        key=lambda item: _RELEASE_MARKER_RE.search(item[:1200]) is None
    )
    for source_text in ordered:
        marker = _RELEASE_MARKER_RE.search(source_text[:1200])
        if marker is None:
            continue
        header = source_text[marker.start() : marker.start() + 700]
        date_match = _DATE_RE.search(header)
        time_match = _TIME_RE.search(header)
        if date_match is None or time_match is None:
            continue
        release_date = (
            f"{int(date_match.group('year')):04d}-"
            f"{_MONTH_NUMBERS[date_match.group('month').lower()]:02d}-"
            f"{int(date_match.group('day')):02d}"
        )
        hour = int(time_match.group("hour"))
        minute = int(time_match.group("minute"))
        released = f"{release_date}T{hour:02d}:{minute:02d}:00"
        local = datetime.fromisoformat(released).replace(
            tzinfo=ZoneInfo("America/New_York")
        )
        released_at_ns = int(local.timestamp() * 1_000_000_000)
        reported_zone = time_match.group("zone").upper()
        consistent = reported_zone == "ET" or reported_zone == local.tzname()
        start = max(0, min(date_match.start(), time_match.start()) - 80)
        finish = min(len(header), max(date_match.end(), time_match.end()) + 80)
        header_lexical = header[start:finish].strip()
        return (
            release_date,
            released,
            released_at_ns,
            (
                reported_zone
                if reported_zone != "ET"
                else cast(str, local.tzname())
            ),
            consistent,
            header_lexical,
        )
    raise ValueError("housing release header is not parseable")


def _parse_source_levels(
    source_text: str, reference_period: str, measure_key: str
) -> tuple[int, int] | None:
    month_name = calendar.month_name[int(reference_period[-2:])]
    previous_period = _previous_month(reference_period)
    previous_name = calendar.month_name[int(previous_period[-2:])]
    if measure_key == HOUSING_BUILDING_PERMITS:
        subject = (
            rf"(?:privately[- ]owned housing units authorized by building permits in "
            rf"{month_name} were at a seasonally adjusted annual rate(?: of)?|"
            rf"the seasonally adjusted annual rate of housing units authorized by "
            rf"building permits in {month_name} was)"
        )
    else:
        noun = "starts" if measure_key == HOUSING_STARTS else "completions"
        subject = (
            rf"privately[- ]owned housing {noun} in {month_name}"
            rf"(?: {reference_period[:4]})? were at a seasonally adjusted annual rate of"
        )
    current_match = re.search(
        subject + r"\s+(?P<value>\d[\d,]*)", source_text, re.IGNORECASE
    )
    if current_match is None:
        return None
    tail = source_text[current_match.end() : current_match.end() + 900]
    previous_match = re.search(
        rf"(?:revised\s+)?{previous_name}(?:\s+{previous_period[:4]})?\s+"
        rf"(?:rate|estimate|figure)(?:\s+{previous_period[:4]})?"
        rf"(?:\s+(?:for|of))?\s+(?P<value>\d[\d,]*)",
        tail,
        re.IGNORECASE,
    )
    current_units = int(current_match.group("value").replace(",", ""))
    if current_units % 1000:
        raise ValueError("housing current level is not whole thousands")
    current = current_units // 1000
    if previous_match is None:
        if re.search(rf"unchanged.{0,160}{previous_name}", tail, re.IGNORECASE):
            return current, current
        return None
    previous_units = int(previous_match.group("value").replace(",", ""))
    if previous_units % 1000:
        raise ValueError("housing previous level is not whole thousands")
    return current, previous_units // 1000


def _ordinary_rows(
    candidates: Sequence[str],
    *,
    reference_period: str,
    publication_kind: str,
) -> tuple[tuple[tuple[str, str, int, str, int], ...], str]:
    keys = (
        (HOUSING_BUILDING_PERMITS, HOUSING_STARTS)
        if publication_kind == HOUSING_SPLIT_STARTS_PERMITS
        else (
            (HOUSING_COMPLETIONS,)
            if publication_kind == HOUSING_SPLIT_COMPLETIONS
            else HOUSING_MEASURE_KEYS
        )
    )
    rows: list[tuple[str, str, int, str, int]] = []
    selected_numbers: list[int] = []
    for key in keys:
        selected: tuple[int, int, int] | None = None
        for candidate_number, source_text in enumerate(candidates):
            try:
                parsed = _parse_source_levels(
                    source_text, reference_period, key
                )
            except ValueError:
                parsed = None
            if parsed is not None:
                selected = (candidate_number, parsed[0], parsed[1])
                break
        if selected is None:
            raise ValueError(
                f"housing levels are not parseable for {reference_period}"
            )
        candidate_number, current, revised = selected
        selected_numbers.append(candidate_number)
        rows.append(
            (
                reference_period,
                key,
                current,
                _previous_month(reference_period),
                revised,
            )
        )
    era = (
        "legacy-split-text"
        if len(candidates) == 1
        else (
            "native-pdf-layout"
            if not any(selected_numbers)
            else "native-pdf-standard"
        )
    )
    return tuple(rows), era


def _artifact(snapshot: OfficialRawSnapshotV1) -> CensusHousingArtifactV1:
    return CensusHousingArtifactV1(
        artifact_uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
    )


def _publication_groups(
    release_index: CensusHousingReleaseIndexV1,
) -> tuple[tuple[CensusHousingReleaseIndexEntryV1, ...], ...]:
    groups: list[tuple[CensusHousingReleaseIndexEntryV1, ...]] = []
    releases = release_index.releases
    index = 0
    while index < len(releases):
        entry = releases[index]
        if entry.reference_period == "2013-09":
            if index + 1 >= len(releases):
                raise ValueError("housing 2013 alias pair is incomplete")
            following = releases[index + 1]
            if (
                entry.artifact_uri != HOUSING_2013_SEPTEMBER_URI
                or following.reference_period != "2013-10"
                or following.artifact_uri != HOUSING_2013_OCTOBER_URI
            ):
                raise ValueError("housing 2013 alias pair differs")
            groups.append((entry, following))
            index += 2
            continue
        groups.append((entry,))
        index += 1
    return tuple(groups)


def _artifact_source_format(
    entry: CensusHousingReleaseIndexEntryV1,
) -> OfficialSourceFormat:
    return (
        OfficialSourceFormat.TEXT
        if entry.publication_kind
        in {HOUSING_SPLIT_STARTS_PERMITS, HOUSING_SPLIT_COMPLETIONS}
        else OfficialSourceFormat.PDF
    )


def build_census_housing_archive_manifest(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    release_index: CensusHousingReleaseIndexV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
) -> CensusHousingArchiveManifestV1:
    """Parse every retained artifact and bind the complete monthly history."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("housing manifest requires a v1 registry")
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("housing manifest requires a v1 U.S. profile")
    if not isinstance(release_index, CensusHousingReleaseIndexV1):
        raise TypeError("housing manifest requires a v1 release index")
    source = registry.source(HOUSING_SOURCE_KEY)
    program = profile.by_key[HOUSING_PROGRAM_KEY]
    if (
        profile.registry_id != registry.registry_id
        or program.source_key != HOUSING_SOURCE_KEY
        or program.archive_uri != HOUSING_INDEX_URI
        or source.archive_uri != HOUSING_INDEX_URI
        or source.parser_id != "official.census-nrc.v1"
    ):
        raise ValueError("housing registry/profile binding differs")
    required_uris = {
        *HOUSING_PREDECESSOR_URIS,
        *(item.artifact_uri for item in release_index.releases),
    }
    if set(snapshots_by_uri) != required_uris:
        missing = sorted(required_uris - set(snapshots_by_uri))
        unexpected = sorted(set(snapshots_by_uri) - required_uris)
        raise ValueError(
            "housing retained corpus differs: "
            f"missing={missing[:3]}, unexpected={unexpected[:3]}"
        )

    predecessors: list[CensusHousingArtifactV1] = []
    known: dict[tuple[str, str], tuple[int, str]] = {}
    for uri in HOUSING_PREDECESSOR_URIS:
        snapshot = snapshots_by_uri[uri]
        _validate_release_snapshot(
            snapshot,
            uri=uri,
            source_format=OfficialSourceFormat.TEXT,
        )
        kind = (
            HOUSING_SPLIT_STARTS_PERMITS
            if "c20_" in uri
            else HOUSING_SPLIT_COMPLETIONS
        )
        candidates = _source_text_candidates(snapshot)
        _release_metadata(candidates)
        rows, era = _ordinary_rows(
            candidates,
            reference_period=HOUSING_PREDECESSOR_REFERENCE_PERIOD,
            publication_kind=kind,
        )
        if era != "legacy-split-text":
            raise ValueError("housing predecessor parser era differs")
        for period, key, current, _, _ in rows:
            known[(period, key)] = (current, uri)
        predecessors.append(_artifact(snapshot))
    if set(known) != {
        (HOUSING_PREDECESSOR_REFERENCE_PERIOD, key)
        for key in HOUSING_MEASURE_KEYS
    }:
        raise ValueError("housing predecessor measure inventory differs")

    publications: list[CensusHousingPublicationV1] = []
    for group in _publication_groups(release_index):
        primary_entry = group[-1]
        primary_snapshot = snapshots_by_uri[primary_entry.artifact_uri]
        primary_format = _artifact_source_format(primary_entry)
        _validate_release_snapshot(
            primary_snapshot,
            uri=primary_entry.artifact_uri,
            source_format=primary_format,
        )
        candidates = _source_text_candidates(primary_snapshot)
        metadata = _release_metadata(candidates)
        artifacts: list[CensusHousingArtifactV1] = []
        entry_ids: list[str] = []
        for entry in group:
            snapshot = snapshots_by_uri[entry.artifact_uri]
            source_format = _artifact_source_format(entry)
            _validate_release_snapshot(
                snapshot,
                uri=entry.artifact_uri,
                source_format=source_format,
            )
            if len(group) == 2:
                alias_candidates = _source_text_candidates(snapshot)
                if _release_metadata(alias_candidates)[:5] != metadata[:5]:
                    raise ValueError("housing 2013 alias timestamps disagree")
                if alias_candidates[-1] != candidates[-1]:
                    raise ValueError(
                        "housing 2013 alias publication text differs"
                    )
            artifacts.append(_artifact(snapshot))
            entry_ids.append(entry.entry_id)

        special_rows = _SPECIAL_VALUES.get(primary_snapshot.content_sha256)
        if primary_entry.reference_period in HOUSING_SPECIAL_PRIMARY_PERIODS:
            if special_rows is None:
                raise ValueError("housing special-case PDF hash differs")
            rows = special_rows
            parser_era = "hash-bound-catch-up-pdf"
        else:
            if special_rows is not None:
                raise ValueError("housing special-case PDF is misclassified")
            rows, parser_era = _ordinary_rows(
                candidates,
                reference_period=primary_entry.reference_period,
                publication_kind=primary_entry.publication_kind,
            )

        prior_known = dict(known)
        measures: list[CensusHousingMeasureV1] = []
        for period, key, current, previous_period, revised_previous in sorted(
            rows, key=lambda item: (item[0], item[1])
        ):
            known_previous = prior_known.get((previous_period, key))
            comparable = known_previous is not None
            measures.append(
                CensusHousingMeasureV1(
                    reference_period=period,
                    measure_key=key,
                    actual_thousands=current,
                    actual_lexical=f"{current:,},000",
                    previous_reference_period=previous_period,
                    revised_previous_thousands=revised_previous,
                    revised_previous_lexical=f"{revised_previous:,},000",
                    previous_as_known_reference_period=(
                        previous_period if comparable else None
                    ),
                    previous_as_known_thousands=(
                        known_previous[0]
                        if known_previous is not None
                        else None
                    ),
                    previous_as_known_lexical=(
                        f"{known_previous[0]:,},000"
                        if known_previous is not None
                        else None
                    ),
                    previous_artifact_uri=(
                        known_previous[1]
                        if known_previous is not None
                        else None
                    ),
                    revision_comparable=comparable,
                    comparison_basis=(
                        "prior-publication"
                        if comparable
                        else "same-publication-catch-up"
                    ),
                )
            )
        for period, key, current, _, _ in rows:
            known[(period, key)] = (current, primary_entry.artifact_uri)
        release_date, released, released_ns, zone, consistent, header = metadata
        publications.append(
            CensusHousingPublicationV1(
                index_entry_ids=tuple(entry_ids),
                primary_reference_period=primary_entry.reference_period,
                release_date=release_date,
                released_lexical=released,
                released_at_ns=released_ns,
                reported_zone=zone,
                zone_consistent=consistent,
                artifacts=tuple(
                    sorted(artifacts, key=lambda item: item.artifact_uri)
                ),
                parser_era=parser_era,
                source_header_lexical=header,
                historical_revision_notice=any(
                    _NOTICE_RE.search(item) is not None for item in candidates
                ),
                catch_up_publication=(
                    primary_entry.reference_period
                    in HOUSING_SPECIAL_PRIMARY_PERIODS
                ),
                federal_disruption_notice=(
                    primary_entry.reference_period
                    in HOUSING_DISRUPTION_PRIMARY_PERIODS
                ),
                measures=tuple(measures),
            )
        )

    target_artifacts = tuple(
        artifact
        for publication in publications
        for artifact in publication.artifacts
    )
    all_artifacts = (
        CensusHousingArtifactV1(
            artifact_uri=release_index.source_uri,
            source_format=OfficialSourceFormat.HTML,
            content_sha256=release_index.content_sha256,
            content_length=release_index.content_length,
        ),
        *predecessors,
        *target_artifacts,
    )
    flattened_measures = tuple(
        measure
        for publication in publications
        for measure in publication.measures
    )
    era_counts = {
        era: sum(publication.parser_era == era for publication in publications)
        for era in (
            "legacy-split-text",
            "native-pdf-layout",
            "native-pdf-standard",
            "hash-bound-catch-up-pdf",
        )
    }
    return CensusHousingArchiveManifestV1(
        registry_id=registry.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        predecessors=tuple(predecessors),
        publications=tuple(publications),
        raw_artifact_count=len(all_artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in all_artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in all_artifacts),
        measure_occurrence_count=len(flattened_measures),
        previous_as_known_measure_count=sum(
            item.previous_as_known_thousands is not None
            for item in flattened_measures
        ),
        comparable_measure_count=sum(
            item.revision_comparable for item in flattened_measures
        ),
        changed_revision_count=sum(
            item.previous_was_revised for item in flattened_measures
        ),
        corrected_link_count=sum(
            item.link_corrected for item in release_index.releases
        ),
        split_text_publication_count=era_counts["legacy-split-text"],
        layout_pdf_publication_count=era_counts["native-pdf-layout"],
        standard_pdf_fallback_count=era_counts["native-pdf-standard"],
        hash_bound_special_publication_count=era_counts[
            "hash-bound-catch-up-pdf"
        ],
        catch_up_publication_count=sum(
            item.catch_up_publication for item in publications
        ),
        disruption_publication_count=sum(
            item.federal_disruption_notice for item in publications
        ),
        historical_revision_notice_count=sum(
            item.historical_revision_notice for item in publications
        ),
        ten_am_publication_count=sum(
            "T10:00:00" in item.released_lexical for item in publications
        ),
        exact_minute_count=len(publications),
        zone_mismatch_count=sum(
            not item.zone_consistent for item in publications
        ),
    )


def replay_census_housing_archive(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    index_snapshot: OfficialRawSnapshotV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    expected: CensusHousingArchiveManifestV1,
) -> CensusHousingArchiveManifestV1:
    """Recompute and compare the complete retained NRC corpus."""
    if not isinstance(expected, CensusHousingArchiveManifestV1):
        raise TypeError("housing replay requires a v1 expected manifest")
    release_index = parse_census_housing_release_index(
        index_snapshot, as_of_date=expected.release_index.as_of_date
    )
    rebuilt = build_census_housing_archive_manifest(
        registry, profile, release_index, snapshots_by_uri
    )
    if rebuilt != expected:
        raise ValueError("housing retained-corpus replay differs")
    return rebuilt


def census_housing_coverage_from_manifest(
    manifest: CensusHousingArchiveManifestV1,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete publication-level coverage for the U.S. profile."""
    if not isinstance(manifest, CensusHousingArchiveManifestV1):
        raise TypeError("housing coverage requires a v1 manifest")
    publication_count = len(manifest.publications)
    previous_publications = sum(
        any(
            measure.previous_as_known_thousands is not None
            for measure in publication.measures
        )
        for publication in manifest.publications
    )
    return UnitedStatesProgramCoverageV1(
        program_key=HOUSING_PROGRAM_KEY,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=manifest.release_index.as_of_date,
        expected_occurrence_count=publication_count,
        schedule_count=publication_count,
        initial_actual_count=publication_count,
        previous_as_known_count=previous_publications,
        revision_count=manifest.changed_revision_count,
        exact_minute_count=publication_count,
        forecast_count=0,
        artifact_sha256s=tuple(
            sorted(
                {
                    manifest.release_index.content_sha256,
                    *(item.content_sha256 for item in manifest.predecessors),
                    *(
                        artifact.content_sha256
                        for publication in manifest.publications
                        for artifact in publication.artifacts
                    ),
                }
            )
        ),
        gap_reasons=(),
        notes=(
            "January 2000 through March 2001 retains separate starts/permits and completions publication times.",
            "The September/October 2013 aliases are one publication; the November report restores delayed starts and completions.",
            "Three 2025-2026 reports retain missed-month initial estimates inside the actual catch-up publication.",
            "Fourteen same-publication prior-month values are not mislabeled as ex-ante previous-as-known observations.",
            "No historical event-level consensus is manufactured from another survey.",
        ),
    )


def packaged_census_housing_archive_manifest_path() -> Path:
    """Return the packaged NRC archive-manifest path."""
    return (
        Path(__file__).with_name("assets")
        / "us_housing_construction_archive_v1.json"
    )


def packaged_census_housing_index_path() -> Path:
    """Return the packaged base64 NRC index-envelope path."""
    return (
        Path(__file__).with_name("assets")
        / "us_housing_construction_index_v1.json"
    )


def load_packaged_census_housing_archive_manifest() -> (
    CensusHousingArchiveManifestV1
):
    """Load and validate the packaged NRC archive manifest."""
    return CensusHousingArchiveManifestV1.from_json(
        packaged_census_housing_archive_manifest_path().read_text(
            encoding="utf-8"
        )
    )


def load_packaged_census_housing_index_snapshot() -> OfficialRawSnapshotV1:
    """Restore the exact packaged NRC archive-page snapshot."""
    try:
        payload = json.loads(
            packaged_census_housing_index_path().read_text(encoding="utf-8")
        )
    except json.JSONDecodeError as exc:
        raise ValueError(
            "packaged housing index envelope is invalid JSON"
        ) from exc
    data = _mapping(payload, "housing index envelope")
    if data.get("schema_version") != HOUSING_INDEX_ENVELOPE_SCHEMA_VERSION:
        raise ValueError("unsupported housing index-envelope schema")
    encoded = data.get("content_base64")
    if (
        not isinstance(encoded, str)
        or not encoded
        or len(encoded) > (MAX_HOUSING_INDEX_BYTES * 4 // 3) + 4
    ):
        raise ValueError("housing index content_base64 is invalid")
    try:
        content = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError(
            "housing index content is not canonical base64"
        ) from exc
    snapshot = OfficialRawSnapshotV1.restore(
        _mapping(data.get("snapshot"), "housing index snapshot"), content
    )
    if snapshot.request.uri != HOUSING_INDEX_URI:
        raise ValueError("packaged housing index URI differs")
    return snapshot


def load_packaged_census_housing_index() -> CensusHousingReleaseIndexV1:
    """Replay the packaged index bytes at the manifest's as-of date."""
    manifest = load_packaged_census_housing_archive_manifest()
    return parse_census_housing_release_index(
        load_packaged_census_housing_index_snapshot(),
        as_of_date=manifest.release_index.as_of_date,
    )


__all__ = [
    "HOUSING_APRIL_2009_ARTIFACT_URI",
    "HOUSING_APRIL_2009_LISTED_URI",
    "HOUSING_AUGUST_2012_ARTIFACT_URI",
    "HOUSING_AUGUST_2012_LISTED_URI",
    "HOUSING_BUILDING_PERMITS",
    "HOUSING_COMPLETIONS",
    "HOUSING_DISRUPTION_PRIMARY_PERIODS",
    "HOUSING_FIRST_REFERENCE_PERIOD",
    "HOUSING_INDEX_URI",
    "HOUSING_MEASURE_KEYS",
    "HOUSING_MISSED_SEPARATE_PERIODS",
    "HOUSING_NOVEMBER_2000_ARTIFACT_URI",
    "HOUSING_NOVEMBER_2000_LISTED_URI",
    "HOUSING_PREDECESSOR_REFERENCE_PERIOD",
    "HOUSING_PREDECESSOR_URIS",
    "HOUSING_PROGRAM_KEY",
    "HOUSING_RELEASE_SCHEDULE_URI",
    "HOUSING_SOURCE_KEY",
    "HOUSING_SPECIAL_PRIMARY_PERIODS",
    "HOUSING_STARTS",
    "CensusHousingArchiveManifestV1",
    "CensusHousingArtifactV1",
    "CensusHousingMeasureV1",
    "CensusHousingPublicationV1",
    "CensusHousingReleaseIndexEntryV1",
    "CensusHousingReleaseIndexV1",
    "build_census_housing_archive_manifest",
    "build_census_housing_archive_requests",
    "build_census_housing_index_request",
    "census_housing_coverage_from_manifest",
    "load_packaged_census_housing_archive_manifest",
    "load_packaged_census_housing_index",
    "load_packaged_census_housing_index_snapshot",
    "packaged_census_housing_archive_manifest_path",
    "packaged_census_housing_index_path",
    "parse_census_housing_release_index",
    "replay_census_housing_archive",
]
