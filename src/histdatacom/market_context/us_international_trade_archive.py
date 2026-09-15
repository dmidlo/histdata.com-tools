"""Occurrence-specific Census/BEA FT-900 international-trade archive.

The official historical index spans monthly PDF releases and annual revision
packages.  This module selects the monthly U.S. International Trade in Goods
and Services publications, preserves bounded index defects, and reconstructs
the initially published and immediately preceding revised deficit for every
reference month from January 2000 onward.
"""

from __future__ import annotations

import base64
import calendar
import hashlib
import html
import json
import math
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
from urllib.parse import urljoin
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

INTERNATIONAL_TRADE_INDEX_ENTRY_SCHEMA_VERSION = (
    "histdatacom.international-trade-index-entry.v1"
)
INTERNATIONAL_TRADE_INDEX_SCHEMA_VERSION = (
    "histdatacom.international-trade-index.v1"
)
INTERNATIONAL_TRADE_ARTIFACT_SCHEMA_VERSION = (
    "histdatacom.international-trade-artifact.v1"
)
INTERNATIONAL_TRADE_MEASURE_SCHEMA_VERSION = (
    "histdatacom.international-trade-measure.v1"
)
INTERNATIONAL_TRADE_PUBLICATION_SCHEMA_VERSION = (
    "histdatacom.international-trade-publication.v1"
)
INTERNATIONAL_TRADE_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.international-trade-archive-manifest.v1"
)
INTERNATIONAL_TRADE_INDEX_ENVELOPE_SCHEMA_VERSION = (
    "histdatacom.international-trade-index-envelope.v1"
)

INTERNATIONAL_TRADE_SOURCE_KEY = "us.bea-census.international-trade"
INTERNATIONAL_TRADE_PROGRAM_KEY = "us.bea-census.international-trade"
INTERNATIONAL_TRADE_INDEX_URI = (
    "https://www.census.gov/foreign-trade/Press-Release/ft900_index.html"
)
INTERNATIONAL_TRADE_RETIRED_INDEX_URI = (
    "https://www.census.gov/foreign-trade/Press-Release/archive.html"
)
INTERNATIONAL_TRADE_RELEASE_SCHEDULE_URI = (
    "https://www.census.gov/economic-indicators/calendar-listview.html"
)
INTERNATIONAL_TRADE_FIRST_REFERENCE_PERIOD = "2000-01"
INTERNATIONAL_TRADE_PREDECESSOR_REFERENCE_PERIOD = "1999-12"
INTERNATIONAL_TRADE_LATEST_PACKAGED_REFERENCE_PERIOD = "2026-07"
INTERNATIONAL_TRADE_MEASURE_KEY = "goods-services-deficit"
INTERNATIONAL_TRADE_PREDECESSOR_URI = (
    "https://www.census.gov/foreign-trade/Press-Release/ft900/" "ft900_9912.pdf"
)
INTERNATIONAL_TRADE_CORRECTED_PERIODS = (
    "2001-08",
    "2001-09",
    "2001-10",
    "2001-11",
    "2002-07",
    "2015-08",
)
INTERNATIONAL_TRADE_SPECIAL_REFERENCE_PERIODS = (
    "2000-05",
    "2000-08",
    "2002-04",
    "2015-07",
)

MAX_INTERNATIONAL_TRADE_INDEX_BYTES = 2 * 1024 * 1024
MAX_INTERNATIONAL_TRADE_ARTIFACT_BYTES = 8 * 1024 * 1024
MAX_INTERNATIONAL_TRADE_RELEASES = 600

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_YEAR_MONTH_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")
_MONTH_NAMES = tuple(calendar.month_name[1:])
_MONTH_NUMBERS = {
    name.lower(): number
    for number, name in enumerate(calendar.month_name)
    if name
}
_DATE_RE = re.compile(
    r"(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(?P<day>\d{1,2})\s*"
    r"[,\.]\s*(?P<year>\d{4})",
    re.IGNORECASE,
)
_TIME_RE = re.compile(
    r"8\s*:\s*30\s*(?:A\.?\s*M\.?)?",
    re.IGNORECASE,
)
_ZONE_RE = re.compile(r"\b(EST|EDT|ET)\b", re.IGNORECASE)
_BALANCE_RE = re.compile(
    r"goods\s+and\s+services\s+deficit\s+(?:of|was)\s+"
    r"\$\s*(?P<actual>\d+(?:\.\d+)?)\s+billion",
    re.IGNORECASE,
)
_MONEY_RE = re.compile(
    r"\$\s*(?P<value>\d+(?:\.\d+)?)\s+billion", re.IGNORECASE
)
_ANNUAL_REVISION_RE = re.compile(
    r"incorporation\s+of\s+annual\s+revisions|"
    r"accompanying\s+.+?annual\s+revision|"
    r"publishing\s+revised\s+statistics\s+on\s+trade",
    re.IGNORECASE | re.DOTALL,
)
_DISRUPTION_RE = re.compile(
    r"lapse\s+in\s+(?:federal\s+)?funding|government\s+shutdown",
    re.IGNORECASE,
)

_SOURCE_WORDS = (
    "goods",
    "services",
    "deficit",
    "billion",
    "revised",
    *_MONTH_NAMES,
)
_SOURCE_WORD_PATTERNS = tuple(
    (
        re.compile(
            r"\b" + r"\s*".join(map(re.escape, word)) + r"\b",
            re.IGNORECASE,
        ),
        word,
    )
    for word in _SOURCE_WORDS
)


def _artifact_uri(period: str) -> str:
    year, month = map(int, period.split("-"))
    return (
        "https://www.census.gov/foreign-trade/Press-Release/ft900/"
        f"ft900_{year % 100:02d}{month:02d}.pdf"
    )


_CORRECTED_LISTED_URIS: Mapping[str, str] = MappingProxyType(
    {
        "2001-08": _artifact_uri("2001-09"),
        "2001-09": _artifact_uri("2001-10"),
        "2001-10": _artifact_uri("2001-11"),
        "2001-11": _artifact_uri("2001-10"),
        "2002-07": _artifact_uri("2003-07"),
        "2015-08": _artifact_uri("2015-09"),
    }
)


@dataclass(frozen=True, slots=True)
class _SpecialRow:
    reference_period: str
    actual_lexical: str
    revised_previous_lexical: str
    release_date: str
    reported_zone: str | None
    source_header_lexical: str


_SPECIAL_ROWS: Mapping[str, _SpecialRow] = MappingProxyType(
    {
        "87d2dba80f31ebc8427af2c45699ff932f66669ca4e448931be39f2cfef17a98": _SpecialRow(
            reference_period="2000-05",
            actual_lexical="31.0",
            revised_previous_lexical="30.5",
            release_date="2000-07-19",
            reported_zone=None,
            source_header_lexical=(
                "This release contains sensitive economic data not to be "
                "released before 8:30 a.m. Wednesday, July 19, 2000"
            ),
        ),
        "5622f4a8ed05df9c79ff0a42af8836202f13fa0c82870cd60b6ab57eba4e2c5f": _SpecialRow(
            reference_period="2000-08",
            actual_lexical="29.4",
            revised_previous_lexical="31.7",
            release_date="2000-10-19",
            reported_zone=None,
            source_header_lexical=(
                "This release contains sensitive economic data not to be "
                "released before 8:30 a.m. Thursday, October 19, 2000"
            ),
        ),
        "8078d696cbc34c21a57b76fc9b728be1320dd792de596e4819670dde9fb40db2": _SpecialRow(
            reference_period="2002-04",
            actual_lexical="35.9",
            revised_previous_lexical="32.5",
            release_date="2002-06-20",
            reported_zone=None,
            source_header_lexical=(
                "This release contains sensitive economic data not to be "
                "released before 8:30 a.m. Thursday, June 20, 2002"
            ),
        ),
        "767f12c242cb6f0e19955c4c59aab49f80fe3a13b61eba498cc6389aab76c283": _SpecialRow(
            reference_period="2015-07",
            actual_lexical="41.9",
            revised_previous_lexical="45.2",
            release_date="2015-09-03",
            reported_zone="EDT",
            source_header_lexical=(
                "FOR IMMEDIATE RELEASE AT 8:30 A.M. EDT, THURSDAY, "
                "SEPTEMBER 3, 2015"
            ),
        ),
    }
)


def _required_text(value: object, name: str) -> str:
    result = str(value or "").strip()
    if not result or len(result.encode("utf-8")) > 16_384:
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
    if not result.startswith("https://"):
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


def _deficit(value: object, lexical: object, name: str) -> tuple[float, str]:
    text = _required_text(lexical, f"{name}_lexical")
    if re.fullmatch(r"(?:0|[1-9]\d{0,3})\.\d", text) is None:
        raise ValueError(f"{name}_lexical is not a one-decimal deficit")
    numeric = float(text)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) != numeric
    ):
        raise ValueError(f"{name} lexical and numeric values differ")
    return numeric, text


def _normalized_source_text(value: str) -> str:
    result = unicodedata.normalize("NFKC", value).replace("\xa0", " ")
    result = result.translate(str.maketrans("‐‑‒–—", "-----"))
    result = re.sub(r"\s+", " ", result).strip()
    for pattern, word in _SOURCE_WORD_PATTERNS:
        result = pattern.sub(word, result)
    return result


class _IndexParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.items: list[tuple[str, tuple[str, ...]]] = []
        self._depth = 0
        self._text: list[str] = []
        self._hrefs: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = {key.lower(): value or "" for key, value in attrs}
        if self._depth:
            if tag.lower() == "div":
                self._depth += 1
            if tag.lower() == "a" and values.get("href"):
                self._hrefs.append(values["href"])
            return
        classes = values.get("class", "").split()
        if tag.lower() == "div" and "uscb-list-item-container" in classes:
            self._depth = 1
            self._text = []
            self._hrefs = []

    def handle_data(self, data: str) -> None:
        if self._depth:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if not self._depth or tag.lower() != "div":
            return
        self._depth -= 1
        if self._depth == 0:
            text = " ".join(" ".join(self._text).split())
            self.items.append((text, tuple(dict.fromkeys(self._hrefs))))


def _index_period(value: str) -> tuple[str, str] | None:
    match = re.match(
        r"^(January|February|Ferbruary|March|April|May|June|July|"
        r"August|September|October|November|December)\s+(\d{4})\b",
        html.unescape(value).strip(),
        re.IGNORECASE,
    )
    if match is None:
        return None
    source_month = match.group(1).title()
    month_name = "February" if source_month == "Ferbruary" else source_month
    period = (
        f"{int(match.group(2)):04d}-{_MONTH_NUMBERS[month_name.lower()]:02d}"
    )
    return period, f"{source_month} {int(match.group(2)):04d}"


@dataclass(frozen=True, slots=True)
class CensusInternationalTradeReleaseIndexEntryV1:
    """One archive-listed monthly FT-900 PDF and bounded correction."""

    reference_period: str
    source_title: str
    listed_uri: str
    artifact_uri: str
    correction_kind: str
    entry_id: str = ""
    schema_version: str = INTERNATIONAL_TRADE_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INTERNATIONAL_TRADE_INDEX_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported international-trade index-entry schema"
            )
        period = _year_month(self.reference_period, "reference_period")
        title = _required_text(self.source_title, "source_title")
        year, month = map(int, period.split("-"))
        expected_title = (
            "Ferbruary 2019"
            if period == "2019-02"
            else f"{calendar.month_name[month]} {year:04d}"
        )
        if title != expected_title:
            raise ValueError("international-trade source title differs")
        listed = _https_uri(self.listed_uri, "listed_uri")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        correction = _required_text(self.correction_kind, "correction_kind")
        expected_artifact = _artifact_uri(period)
        expected_listed = _CORRECTED_LISTED_URIS.get(period, expected_artifact)
        expected_correction = (
            "wrong-target" if period in _CORRECTED_LISTED_URIS else "none"
        )
        if (listed, artifact, correction) != (
            expected_listed,
            expected_artifact,
            expected_correction,
        ):
            raise ValueError("international-trade link correction differs")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "listed_uri", listed)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "correction_kind", correction)
        expected = _stable_id(
            "international-trade-index-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError("international-trade index-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    @property
    def link_corrected(self) -> bool:
        return self.correction_kind != "none"

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "source_title": self.source_title,
            "listed_uri": self.listed_uri,
            "artifact_uri": self.artifact_uri,
            "correction_kind": self.correction_kind,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusInternationalTradeReleaseIndexEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            source_title=str(data.get("source_title", "")),
            listed_uri=str(data.get("listed_uri", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            correction_kind=str(data.get("correction_kind", "")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusInternationalTradeReleaseIndexV1:
    """Hash-bound enumeration of every target monthly FT-900 release."""

    source_uri: str
    content_sha256: str
    content_length: int
    as_of_date: str
    releases: tuple[CensusInternationalTradeReleaseIndexEntryV1, ...]
    index_id: str = ""
    schema_version: str = INTERNATIONAL_TRADE_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INTERNATIONAL_TRADE_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported international-trade index schema")
        if (
            _https_uri(self.source_uri, "source_uri")
            != INTERNATIONAL_TRADE_INDEX_URI
        ):
            raise ValueError("international-trade index URI differs")
        sha = _sha256(self.content_sha256, "content_sha256")
        _positive_int(
            self.content_length,
            "content_length",
            MAX_INTERNATIONAL_TRADE_INDEX_BYTES,
        )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        releases = tuple(self.releases)
        if (
            not releases
            or len(releases) > MAX_INTERNATIONAL_TRADE_RELEASES
            or any(
                not isinstance(
                    item, CensusInternationalTradeReleaseIndexEntryV1
                )
                for item in releases
            )
        ):
            raise TypeError("international-trade index releases are invalid")
        periods = tuple(item.reference_period for item in releases)
        if periods != tuple(sorted(set(periods))):
            raise ValueError(
                "international-trade index is not uniquely ordered"
            )
        if periods[0] != INTERNATIONAL_TRADE_FIRST_REFERENCE_PERIOD:
            raise ValueError(
                "international-trade index begins at the wrong period"
            )
        if periods[-1] < INTERNATIONAL_TRADE_LATEST_PACKAGED_REFERENCE_PERIOD:
            raise ValueError(
                "international-trade index ends before qualified corpus"
            )
        expected: list[str] = []
        period = INTERNATIONAL_TRADE_FIRST_REFERENCE_PERIOD
        while period <= periods[-1]:
            expected.append(period)
            period = _next_month(period)
        if periods != tuple(expected):
            raise ValueError("international-trade index has a period gap")
        corrected = tuple(
            item.reference_period for item in releases if item.link_corrected
        )
        if corrected != INTERNATIONAL_TRADE_CORRECTED_PERIODS:
            raise ValueError("international-trade correction inventory differs")
        if len({item.artifact_uri for item in releases}) != len(releases):
            raise ValueError(
                "international-trade index repeats an artifact URI"
            )
        object.__setattr__(self, "content_sha256", sha)
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "releases", releases)
        expected_id = _stable_id(
            "international-trade-index", self.identity_payload()
        )
        if self.index_id and self.index_id != expected_id:
            raise ValueError("international-trade index identity differs")
        object.__setattr__(self, "index_id", expected_id)

    @property
    def by_period(
        self,
    ) -> Mapping[str, CensusInternationalTradeReleaseIndexEntryV1]:
        return MappingProxyType(
            {item.reference_period: item for item in self.releases}
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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusInternationalTradeReleaseIndexV1:
        return cls(
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            as_of_date=str(data.get("as_of_date", "")),
            releases=tuple(
                CensusInternationalTradeReleaseIndexEntryV1.from_dict(
                    _mapping(item, "international-trade index entry")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusInternationalTradeArtifactV1:
    """Compact hash evidence for one retained Census FT-900 PDF."""

    artifact_uri: str
    content_sha256: str
    content_length: int
    artifact_id: str = ""
    schema_version: str = INTERNATIONAL_TRADE_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INTERNATIONAL_TRADE_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported international-trade artifact schema")
        uri = _https_uri(self.artifact_uri, "artifact_uri")
        sha = _sha256(self.content_sha256, "content_sha256")
        _positive_int(
            self.content_length,
            "content_length",
            MAX_INTERNATIONAL_TRADE_ARTIFACT_BYTES,
        )
        object.__setattr__(self, "artifact_uri", uri)
        object.__setattr__(self, "content_sha256", sha)
        expected = _stable_id(
            "international-trade-artifact", self.identity_payload()
        )
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("international-trade artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "artifact_uri": self.artifact_uri,
            "source_format": OfficialSourceFormat.PDF.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusInternationalTradeArtifactV1:
        if data.get("source_format") != OfficialSourceFormat.PDF.value:
            raise ValueError("international-trade artifact format differs")
        return cls(
            artifact_uri=str(data.get("artifact_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusInternationalTradeMeasureV1:
    """One published deficit and its immediately preceding vintage."""

    measure_key: str
    reference_period: str
    actual_deficit_billions: float
    actual_lexical: str
    previous_reference_period: str
    revised_previous_deficit_billions: float
    revised_previous_lexical: str
    previous_as_known_reference_period: str
    previous_as_known_deficit_billions: float
    previous_as_known_lexical: str
    previous_artifact_uri: str
    annual_revision_context: bool
    comparison_basis: str
    measure_id: str = ""
    schema_version: str = INTERNATIONAL_TRADE_MEASURE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INTERNATIONAL_TRADE_MEASURE_SCHEMA_VERSION:
            raise ValueError("unsupported international-trade measure schema")
        key = _required_text(self.measure_key, "measure_key")
        if key != INTERNATIONAL_TRADE_MEASURE_KEY:
            raise ValueError("international-trade measure key differs")
        period = _year_month(self.reference_period, "reference_period")
        actual, actual_text = _deficit(
            self.actual_deficit_billions, self.actual_lexical, "actual"
        )
        previous = _year_month(
            self.previous_reference_period, "previous_reference_period"
        )
        if previous != _previous_month(period):
            raise ValueError("international-trade previous period differs")
        revised, revised_text = _deficit(
            self.revised_previous_deficit_billions,
            self.revised_previous_lexical,
            "revised_previous",
        )
        known_period = _year_month(
            self.previous_as_known_reference_period,
            "previous_as_known_reference_period",
        )
        if known_period != previous:
            raise ValueError("international-trade known period differs")
        known, known_text = _deficit(
            self.previous_as_known_deficit_billions,
            self.previous_as_known_lexical,
            "previous_as_known",
        )
        previous_uri = _https_uri(
            self.previous_artifact_uri, "previous_artifact_uri"
        )
        if not isinstance(self.annual_revision_context, bool):
            raise TypeError("annual_revision_context must be boolean")
        annual = period.endswith("-04")
        if self.annual_revision_context != annual:
            raise ValueError(
                "international-trade annual-revision context differs"
            )
        basis = _required_text(self.comparison_basis, "comparison_basis")
        expected_basis = (
            "concurrent-annual-revision"
            if annual
            else "prior-monthly-publication"
        )
        if basis != expected_basis:
            raise ValueError("international-trade comparison basis differs")
        object.__setattr__(self, "measure_key", key)
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "actual_deficit_billions", actual)
        object.__setattr__(self, "actual_lexical", actual_text)
        object.__setattr__(self, "previous_reference_period", previous)
        object.__setattr__(self, "revised_previous_deficit_billions", revised)
        object.__setattr__(self, "revised_previous_lexical", revised_text)
        object.__setattr__(
            self, "previous_as_known_reference_period", known_period
        )
        object.__setattr__(self, "previous_as_known_deficit_billions", known)
        object.__setattr__(self, "previous_as_known_lexical", known_text)
        object.__setattr__(self, "previous_artifact_uri", previous_uri)
        object.__setattr__(self, "comparison_basis", basis)
        expected = _stable_id(
            "international-trade-measure", self.identity_payload()
        )
        if self.measure_id and self.measure_id != expected:
            raise ValueError("international-trade measure identity differs")
        object.__setattr__(self, "measure_id", expected)

    @property
    def previous_was_revised(self) -> bool:
        return (
            self.previous_as_known_deficit_billions
            != self.revised_previous_deficit_billions
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "measure_key": self.measure_key,
            "reference_period": self.reference_period,
            "actual_deficit_billions": self.actual_deficit_billions,
            "actual_lexical": self.actual_lexical,
            "previous_reference_period": self.previous_reference_period,
            "revised_previous_deficit_billions": (
                self.revised_previous_deficit_billions
            ),
            "revised_previous_lexical": self.revised_previous_lexical,
            "previous_as_known_reference_period": (
                self.previous_as_known_reference_period
            ),
            "previous_as_known_deficit_billions": (
                self.previous_as_known_deficit_billions
            ),
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "previous_artifact_uri": self.previous_artifact_uri,
            "annual_revision_context": self.annual_revision_context,
            "comparison_basis": self.comparison_basis,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "measure_id": self.measure_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusInternationalTradeMeasureV1:
        return cls(
            measure_key=str(data.get("measure_key", "")),
            reference_period=str(data.get("reference_period", "")),
            actual_deficit_billions=cast(
                float, data.get("actual_deficit_billions")
            ),
            actual_lexical=str(data.get("actual_lexical", "")),
            previous_reference_period=str(
                data.get("previous_reference_period", "")
            ),
            revised_previous_deficit_billions=cast(
                float, data.get("revised_previous_deficit_billions")
            ),
            revised_previous_lexical=str(
                data.get("revised_previous_lexical", "")
            ),
            previous_as_known_reference_period=str(
                data.get("previous_as_known_reference_period", "")
            ),
            previous_as_known_deficit_billions=cast(
                float, data.get("previous_as_known_deficit_billions")
            ),
            previous_as_known_lexical=str(
                data.get("previous_as_known_lexical", "")
            ),
            previous_artifact_uri=str(data.get("previous_artifact_uri", "")),
            annual_revision_context=cast(
                bool, data.get("annual_revision_context")
            ),
            comparison_basis=str(data.get("comparison_basis", "")),
            measure_id=str(data.get("measure_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusInternationalTradePublicationV1:
    """One real FT-900 publication and its normalized headline measure."""

    index_entry_id: str
    reference_period: str
    release_date: str
    released_lexical: str
    released_at_ns: int
    reported_zone: str | None
    zone_basis: str
    zone_consistent: bool | None
    artifact: CensusInternationalTradeArtifactV1
    parser_era: str
    source_header_lexical: str
    annual_revision_context: bool
    federal_disruption_notice: bool
    measure: CensusInternationalTradeMeasureV1
    publication_id: str = ""
    schema_version: str = INTERNATIONAL_TRADE_PUBLICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INTERNATIONAL_TRADE_PUBLICATION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported international-trade publication schema"
            )
        entry_id = _required_text(self.index_entry_id, "index_entry_id")
        if not entry_id.startswith("international-trade-index-entry:sha256:"):
            raise ValueError("international-trade index-entry identity differs")
        period = _year_month(self.reference_period, "reference_period")
        release_date = _iso_date(self.release_date, "release_date")
        reference_end = date.fromisoformat(_month_end(period))
        release_day = date.fromisoformat(release_date)
        if not reference_end < release_day:
            raise ValueError(
                "international-trade release precedes reference month"
            )
        lag_months = (release_day.year * 12 + release_day.month) - (
            reference_end.year * 12 + reference_end.month
        )
        if lag_months > 5:
            raise ValueError(
                "international-trade release lag is outside bounds"
            )
        lexical = _required_text(self.released_lexical, "released_lexical")
        if lexical != f"{release_date}T08:30:00":
            raise ValueError("international-trade release lexical differs")
        local = datetime.fromisoformat(lexical).replace(
            tzinfo=ZoneInfo("America/New_York")
        )
        expected_ns = int(local.timestamp() * 1_000_000_000)
        if (
            not isinstance(self.released_at_ns, int)
            or isinstance(self.released_at_ns, bool)
            or self.released_at_ns != expected_ns
        ):
            raise ValueError("international-trade normalized timestamp differs")
        zone = _optional_text(self.reported_zone)
        basis = _required_text(self.zone_basis, "zone_basis")
        if zone is None:
            if (
                basis != "producer-local-inference"
                or self.zone_consistent is not None
            ):
                raise ValueError(
                    "international-trade inferred-zone state differs"
                )
        else:
            zone = zone.upper()
            if zone not in {"EST", "EDT", "ET"} or basis != "source-token":
                raise ValueError("international-trade reported zone differs")
            expected_consistency = zone == "ET" or zone == local.tzname()
            if self.zone_consistent is not expected_consistency:
                raise ValueError("international-trade zone consistency differs")
        if not isinstance(self.artifact, CensusInternationalTradeArtifactV1):
            raise TypeError("international-trade publication artifact differs")
        parser_era = _required_text(self.parser_era, "parser_era")
        expected_era = (
            "hash-bound-pdf"
            if period in INTERNATIONAL_TRADE_SPECIAL_REFERENCE_PERIODS
            else "native-pdf-standard"
        )
        if parser_era != expected_era:
            raise ValueError("international-trade parser era differs")
        header = _required_text(
            self.source_header_lexical, "source_header_lexical"
        )
        for name in ("annual_revision_context", "federal_disruption_notice"):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        annual = period.endswith("-04")
        if self.annual_revision_context != annual:
            raise ValueError("international-trade annual revision differs")
        if (
            not isinstance(self.measure, CensusInternationalTradeMeasureV1)
            or self.measure.reference_period != period
            or self.measure.annual_revision_context != annual
            or self.artifact.artifact_uri != _artifact_uri(period)
        ):
            raise ValueError("international-trade publication binding differs")
        object.__setattr__(self, "index_entry_id", entry_id)
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "released_lexical", lexical)
        object.__setattr__(self, "reported_zone", zone)
        object.__setattr__(self, "zone_basis", basis)
        object.__setattr__(self, "parser_era", parser_era)
        object.__setattr__(self, "source_header_lexical", header)
        expected = _stable_id(
            "international-trade-publication", self.identity_payload()
        )
        if self.publication_id and self.publication_id != expected:
            raise ValueError("international-trade publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "index_entry_id": self.index_entry_id,
            "reference_period": self.reference_period,
            "release_date": self.release_date,
            "released_lexical": self.released_lexical,
            "released_at_ns": self.released_at_ns,
            "reported_zone": self.reported_zone,
            "zone_basis": self.zone_basis,
            "zone_consistent": self.zone_consistent,
            "artifact": self.artifact.to_dict(),
            "parser_era": self.parser_era,
            "source_header_lexical": self.source_header_lexical,
            "annual_revision_context": self.annual_revision_context,
            "federal_disruption_notice": self.federal_disruption_notice,
            "measure": self.measure.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "publication_id": self.publication_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusInternationalTradePublicationV1:
        zone_consistent = data.get("zone_consistent")
        if zone_consistent is not None and not isinstance(
            zone_consistent, bool
        ):
            raise TypeError("zone_consistent must be boolean or null")
        return cls(
            index_entry_id=str(data.get("index_entry_id", "")),
            reference_period=str(data.get("reference_period", "")),
            release_date=str(data.get("release_date", "")),
            released_lexical=str(data.get("released_lexical", "")),
            released_at_ns=cast(int, data.get("released_at_ns")),
            reported_zone=_optional_text(data.get("reported_zone")),
            zone_basis=str(data.get("zone_basis", "")),
            zone_consistent=zone_consistent,
            artifact=CensusInternationalTradeArtifactV1.from_dict(
                _mapping(data.get("artifact"), "international-trade artifact")
            ),
            parser_era=str(data.get("parser_era", "")),
            source_header_lexical=str(data.get("source_header_lexical", "")),
            annual_revision_context=cast(
                bool, data.get("annual_revision_context")
            ),
            federal_disruption_notice=cast(
                bool, data.get("federal_disruption_notice")
            ),
            measure=CensusInternationalTradeMeasureV1.from_dict(
                _mapping(data.get("measure"), "international-trade measure")
            ),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusInternationalTradeArchiveManifestV1:
    """Compact replay receipt for the complete monthly FT-900 corpus."""

    registry_id: str
    profile_id: str
    release_index: CensusInternationalTradeReleaseIndexV1
    predecessor: CensusInternationalTradeArtifactV1
    predecessor_actual_deficit_billions: float
    predecessor_actual_lexical: str
    publications: tuple[CensusInternationalTradePublicationV1, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    measure_occurrence_count: int
    previous_as_known_measure_count: int
    changed_revision_count: int
    corrected_link_count: int
    native_pdf_publication_count: int
    hash_bound_publication_count: int
    inferred_zone_count: int
    source_zone_token_count: int
    zone_mismatch_count: int
    annual_revision_count: int
    federal_disruption_notice_count: int
    exact_minute_count: int
    manifest_id: str = ""
    schema_version: str = INTERNATIONAL_TRADE_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INTERNATIONAL_TRADE_ARCHIVE_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError("unsupported international-trade manifest schema")
        registry_id = _required_text(self.registry_id, "registry_id")
        profile_id = _required_text(self.profile_id, "profile_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("international-trade registry identity differs")
        if not profile_id.startswith("us-backfill-profile:sha256:"):
            raise ValueError("international-trade profile identity differs")
        if not isinstance(
            self.release_index, CensusInternationalTradeReleaseIndexV1
        ):
            raise TypeError("international-trade manifest index differs")
        if (
            not isinstance(self.predecessor, CensusInternationalTradeArtifactV1)
            or self.predecessor.artifact_uri
            != INTERNATIONAL_TRADE_PREDECESSOR_URI
        ):
            raise ValueError("international-trade predecessor differs")
        predecessor_value, predecessor_text = _deficit(
            self.predecessor_actual_deficit_billions,
            self.predecessor_actual_lexical,
            "predecessor_actual",
        )
        publications = tuple(self.publications)
        if len(publications) != len(self.release_index.releases) or any(
            not isinstance(item, CensusInternationalTradePublicationV1)
            for item in publications
        ):
            raise ValueError(
                "international-trade publication inventory differs"
            )
        times = tuple(item.released_at_ns for item in publications)
        if times != tuple(sorted(set(times))):
            raise ValueError(
                "international-trade publication chronology differs"
            )
        entries = self.release_index.by_period
        known: dict[str, tuple[float, str, str]] = {
            INTERNATIONAL_TRADE_PREDECESSOR_REFERENCE_PERIOD: (
                predecessor_value,
                predecessor_text,
                self.predecessor.artifact_uri,
            )
        }
        for publication in publications:
            entry = entries.get(publication.reference_period)
            if (
                entry is None
                or publication.index_entry_id != entry.entry_id
                or publication.artifact.artifact_uri != entry.artifact_uri
                or publication.release_date > self.release_index.as_of_date
            ):
                raise ValueError("international-trade index coverage differs")
            previous = known.get(publication.measure.previous_reference_period)
            if (
                previous is None
                or (
                    publication.measure.previous_as_known_deficit_billions,
                    publication.measure.previous_as_known_lexical,
                    publication.measure.previous_artifact_uri,
                )
                != previous
            ):
                raise ValueError("international-trade prior lineage differs")
            known[publication.reference_period] = (
                publication.measure.actual_deficit_billions,
                publication.measure.actual_lexical,
                publication.artifact.artifact_uri,
            )
        periods = tuple(item.reference_period for item in publications)
        if periods != tuple(entries):
            raise ValueError("international-trade monthly coverage differs")
        artifacts = (self.predecessor,) + tuple(
            item.artifact for item in publications
        )
        all_hashes = {
            self.release_index.content_sha256,
            *(item.content_sha256 for item in artifacts),
        }
        if len(all_hashes) != len(artifacts) + 1:
            raise ValueError("international-trade corpus repeats source bytes")
        observed = {
            "raw_artifact_count": len(artifacts) + 1,
            "unique_content_sha256_count": len(all_hashes),
            "total_content_bytes": self.release_index.content_length
            + sum(item.content_length for item in artifacts),
            "measure_occurrence_count": len(publications),
            "previous_as_known_measure_count": len(publications),
            "changed_revision_count": sum(
                item.measure.previous_was_revised for item in publications
            ),
            "corrected_link_count": sum(
                item.link_corrected for item in self.release_index.releases
            ),
            "native_pdf_publication_count": sum(
                item.parser_era == "native-pdf-standard"
                for item in publications
            ),
            "hash_bound_publication_count": sum(
                item.parser_era == "hash-bound-pdf" for item in publications
            ),
            "inferred_zone_count": sum(
                item.reported_zone is None for item in publications
            ),
            "source_zone_token_count": sum(
                item.reported_zone is not None for item in publications
            ),
            "zone_mismatch_count": sum(
                item.zone_consistent is False for item in publications
            ),
            "annual_revision_count": sum(
                item.annual_revision_context for item in publications
            ),
            "federal_disruption_notice_count": sum(
                item.federal_disruption_notice for item in publications
            ),
            "exact_minute_count": len(publications),
        }
        for name, expected in observed.items():
            actual = getattr(self, name)
            if (
                not isinstance(actual, int)
                or isinstance(actual, bool)
                or actual != expected
            ):
                raise ValueError(f"international-trade manifest {name} differs")
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(
            self, "predecessor_actual_deficit_billions", predecessor_value
        )
        object.__setattr__(self, "predecessor_actual_lexical", predecessor_text)
        object.__setattr__(self, "publications", publications)
        expected_id = _stable_id(
            "international-trade-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected_id:
            raise ValueError("international-trade manifest identity differs")
        object.__setattr__(self, "manifest_id", expected_id)

    @property
    def by_period(
        self,
    ) -> Mapping[str, CensusInternationalTradePublicationV1]:
        return MappingProxyType(
            {item.reference_period: item for item in self.publications}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "predecessor": self.predecessor.to_dict(),
            "predecessor_actual_deficit_billions": (
                self.predecessor_actual_deficit_billions
            ),
            "predecessor_actual_lexical": self.predecessor_actual_lexical,
            "publications": [item.to_dict() for item in self.publications],
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "measure_occurrence_count": self.measure_occurrence_count,
            "previous_as_known_measure_count": (
                self.previous_as_known_measure_count
            ),
            "changed_revision_count": self.changed_revision_count,
            "corrected_link_count": self.corrected_link_count,
            "native_pdf_publication_count": self.native_pdf_publication_count,
            "hash_bound_publication_count": self.hash_bound_publication_count,
            "inferred_zone_count": self.inferred_zone_count,
            "source_zone_token_count": self.source_zone_token_count,
            "zone_mismatch_count": self.zone_mismatch_count,
            "annual_revision_count": self.annual_revision_count,
            "federal_disruption_notice_count": (
                self.federal_disruption_notice_count
            ),
            "exact_minute_count": self.exact_minute_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusInternationalTradeArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=CensusInternationalTradeReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "international-trade index")
            ),
            predecessor=CensusInternationalTradeArtifactV1.from_dict(
                _mapping(
                    data.get("predecessor"), "international-trade predecessor"
                )
            ),
            predecessor_actual_deficit_billions=cast(
                float, data.get("predecessor_actual_deficit_billions")
            ),
            predecessor_actual_lexical=str(
                data.get("predecessor_actual_lexical", "")
            ),
            publications=tuple(
                CensusInternationalTradePublicationV1.from_dict(
                    _mapping(item, "international-trade publication")
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
            changed_revision_count=cast(
                int, data.get("changed_revision_count")
            ),
            corrected_link_count=cast(int, data.get("corrected_link_count")),
            native_pdf_publication_count=cast(
                int, data.get("native_pdf_publication_count")
            ),
            hash_bound_publication_count=cast(
                int, data.get("hash_bound_publication_count")
            ),
            inferred_zone_count=cast(int, data.get("inferred_zone_count")),
            source_zone_token_count=cast(
                int, data.get("source_zone_token_count")
            ),
            zone_mismatch_count=cast(int, data.get("zone_mismatch_count")),
            annual_revision_count=cast(int, data.get("annual_revision_count")),
            federal_disruption_notice_count=cast(
                int, data.get("federal_disruption_notice_count")
            ),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> CensusInternationalTradeArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "international-trade manifest is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload, "international-trade manifest"))


def build_census_international_trade_index_request(
    registry: OfficialSourceRegistryV1, *, as_of_date: str
) -> OfficialSourceRequestV1:
    """Plan the bounded official FT-900 historical-index request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("international-trade index requires a v1 registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(INTERNATIONAL_TRADE_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=INTERNATIONAL_TRADE_INDEX_URI,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=US_BACKFILL_START_DATE,
        window_end=as_of,
    )


def parse_census_international_trade_release_index(
    snapshot: OfficialRawSnapshotV1, *, as_of_date: str
) -> CensusInternationalTradeReleaseIndexV1:
    """Parse the retained Census FT-900 index without following links."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("international-trade index requires a v1 snapshot")
    as_of = _iso_date(as_of_date, "as_of_date")
    if (
        snapshot.request.source_key != INTERNATIONAL_TRADE_SOURCE_KEY
        or snapshot.request.uri != INTERNATIONAL_TRADE_INDEX_URI
        or snapshot.request.source_format is not OfficialSourceFormat.HTML
        or snapshot.request.window_start != US_BACKFILL_START_DATE
        or snapshot.request.window_end != as_of
        or snapshot.status_code != 200
        or len(snapshot.content) > MAX_INTERNATIONAL_TRADE_INDEX_BYTES
    ):
        raise ValueError("international-trade index snapshot differs")
    try:
        source_text = snapshot.content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("international-trade index is not UTF-8") from exc
    parser = _IndexParser()
    parser.feed(source_text)
    parser.close()
    observed: dict[str, tuple[str, str]] = {}
    for title_text, hrefs in parser.items:
        parsed = _index_period(title_text)
        if parsed is None:
            continue
        period, source_title = parsed
        if period < INTERNATIONAL_TRADE_FIRST_REFERENCE_PERIOD:
            continue
        pdfs = tuple(
            dict.fromkeys(
                urljoin(
                    INTERNATIONAL_TRADE_INDEX_URI, html.unescape(href).strip()
                )
                for href in hrefs
                if html.unescape(href).lower().split("?", 1)[0].endswith(".pdf")
            )
        )
        if len(pdfs) != 1:
            raise ValueError(
                "international-trade monthly index PDF count differs"
            )
        prior = observed.setdefault(period, (source_title, pdfs[0]))
        if prior != (source_title, pdfs[0]):
            raise ValueError("international-trade index repeats a period")
    if not observed:
        raise ValueError(
            "international-trade index contains no monthly releases"
        )
    entries = tuple(
        CensusInternationalTradeReleaseIndexEntryV1(
            reference_period=period,
            source_title=title,
            listed_uri=listed,
            artifact_uri=_artifact_uri(period),
            correction_kind=(
                "wrong-target" if period in _CORRECTED_LISTED_URIS else "none"
            ),
        )
        for period, (title, listed) in sorted(observed.items())
    )
    return CensusInternationalTradeReleaseIndexV1(
        source_uri=INTERNATIONAL_TRADE_INDEX_URI,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        as_of_date=as_of,
        releases=entries,
    )


def _release_request(
    registry: OfficialSourceRegistryV1, *, uri: str, period: str
) -> OfficialSourceRequestV1:
    source = registry.source(INTERNATIONAL_TRADE_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=OfficialSourceFormat.PDF,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=f"{period}-01",
        window_end=_month_end(period),
    )


def build_census_international_trade_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: CensusInternationalTradeReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan the predecessor and all monthly FT-900 PDF requests."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("international-trade requests require a v1 registry")
    if not isinstance(release_index, CensusInternationalTradeReleaseIndexV1):
        raise TypeError("international-trade requests require a v1 index")
    requests = (
        _release_request(
            registry,
            uri=INTERNATIONAL_TRADE_PREDECESSOR_URI,
            period=INTERNATIONAL_TRADE_PREDECESSOR_REFERENCE_PERIOD,
        ),
        *(
            _release_request(
                registry, uri=item.artifact_uri, period=item.reference_period
            )
            for item in release_index.releases
        ),
    )
    if len({item.uri for item in requests}) != len(requests):
        raise ValueError("international-trade request plan repeats an artifact")
    return requests


def _validate_release_snapshot(
    snapshot: OfficialRawSnapshotV1, *, uri: str
) -> None:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("international-trade release requires a v1 snapshot")
    if (
        snapshot.request.source_key != INTERNATIONAL_TRADE_SOURCE_KEY
        or snapshot.request.uri != uri
        or snapshot.request.source_format is not OfficialSourceFormat.PDF
        or snapshot.status_code != 200
        or not snapshot.content.startswith(b"%PDF-")
        or len(snapshot.content) > MAX_INTERNATIONAL_TRADE_ARTIFACT_BYTES
    ):
        raise ValueError("international-trade release snapshot differs")


def _source_text(content: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(content), strict=False)
        pages = tuple(
            reader.pages[index].extract_text() or ""
            for index in range(min(5, len(reader.pages)))
        )
    except Exception as exc:
        raise ValueError("international-trade PDF is not readable") from exc
    result = _normalized_source_text("\n".join(pages))
    if not result:
        raise ValueError(
            "international-trade PDF has no extractable first pages"
        )
    return result


def _release_metadata(
    source_text: str, *, reference_period: str
) -> tuple[str, str, int, str | None, str, bool | None, str]:
    time_match = _TIME_RE.search(source_text[:8000])
    if time_match is None:
        raise ValueError("international-trade release time is not parseable")
    year, month = map(int, reference_period.split("-"))
    reference_end = date(year, month, calendar.monthrange(year, month)[1])
    candidates: list[tuple[int, re.Match[str], date]] = []
    for match in _DATE_RE.finditer(source_text[:8000]):
        parsed = date(
            int(match.group("year")),
            _MONTH_NUMBERS[match.group("month").lower()],
            int(match.group("day")),
        )
        lag_months = (parsed.year * 12 + parsed.month) - (
            reference_end.year * 12 + reference_end.month
        )
        if parsed > reference_end and 0 <= lag_months <= 5:
            candidates.append(
                (abs(match.start() - time_match.start()), match, parsed)
            )
    if not candidates:
        raise ValueError("international-trade release date is not parseable")
    distance, date_match, released_date = min(
        candidates, key=lambda item: item[0]
    )
    if distance > 50:
        raise ValueError("international-trade release header is ambiguous")
    release_date = released_date.isoformat()
    lexical = f"{release_date}T08:30:00"
    local = datetime.fromisoformat(lexical).replace(
        tzinfo=ZoneInfo("America/New_York")
    )
    window = source_text[
        max(0, time_match.start() - 100) : time_match.end() + 200
    ]
    zone_match = _ZONE_RE.search(window)
    zone = zone_match.group(1).upper() if zone_match is not None else None
    basis = "source-token" if zone is not None else "producer-local-inference"
    consistent = (
        None if zone is None else zone == "ET" or zone == local.tzname()
    )
    start = max(0, min(time_match.start(), date_match.start()) - 180)
    end = min(len(source_text), max(time_match.end(), date_match.end()) + 100)
    return (
        release_date,
        lexical,
        int(local.timestamp() * 1_000_000_000),
        zone,
        basis,
        consistent,
        source_text[start:end].strip(),
    )


def _source_values(
    source_text: str, *, reference_period: str
) -> tuple[str, str]:
    numeric_text = re.sub(r"(?<=\d)\s+(?=\d)", "", source_text)
    current = _BALANCE_RE.search(numeric_text)
    if current is None:
        raise ValueError(
            "international-trade headline deficit is not parseable"
        )
    year, month = map(int, reference_period.split("-"))
    current_name = calendar.month_name[month]
    context = numeric_text[
        max(0, current.start() - 1_500) : current.end() + 350
    ]
    if (
        re.search(
            rf"\b{current_name}\b(?:\s+AND\s+ANNUAL)?\s*,?\s*{year}\b",
            context,
            re.IGNORECASE,
        )
        is None
    ):
        raise ValueError("international-trade reference-period header differs")
    previous_period = _previous_month(reference_period)
    previous_name = calendar.month_name[int(previous_period[-2:])]
    tail = numeric_text[current.end() : current.end() + 550]
    end_match = re.search(
        rf"(?:in|from)\s+{previous_name}\s*,?\s*revised",
        tail,
        re.IGNORECASE,
    )
    if end_match is None:
        raise ValueError("international-trade revised-prior phrase differs")
    segment = tail[: end_match.end()]
    values = tuple(
        match.group("value") for match in _MONEY_RE.finditer(segment)
    )
    if values:
        previous = values[-1]
    elif re.search(r"virtually\s+unchanged\s+from", segment, re.IGNORECASE):
        previous = current.group("actual")
    else:
        raise ValueError("international-trade revised-prior value is absent")
    return current.group("actual"), previous


def _artifact(
    snapshot: OfficialRawSnapshotV1,
) -> CensusInternationalTradeArtifactV1:
    return CensusInternationalTradeArtifactV1(
        artifact_uri=snapshot.request.uri,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
    )


def _special_metadata(
    row: _SpecialRow,
) -> tuple[str, str, int, str | None, str, bool | None, str]:
    lexical = f"{row.release_date}T08:30:00"
    local = datetime.fromisoformat(lexical).replace(
        tzinfo=ZoneInfo("America/New_York")
    )
    zone = row.reported_zone
    return (
        row.release_date,
        lexical,
        int(local.timestamp() * 1_000_000_000),
        zone,
        "source-token" if zone is not None else "producer-local-inference",
        None if zone is None else zone == "ET" or zone == local.tzname(),
        row.source_header_lexical,
    )


def build_census_international_trade_archive_manifest(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    release_index: CensusInternationalTradeReleaseIndexV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
) -> CensusInternationalTradeArchiveManifestV1:
    """Parse and bind the complete occurrence-specific monthly corpus."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("international-trade manifest requires a v1 registry")
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError(
            "international-trade manifest requires a v1 U.S. profile"
        )
    if not isinstance(release_index, CensusInternationalTradeReleaseIndexV1):
        raise TypeError("international-trade manifest requires a v1 index")
    source = registry.source(INTERNATIONAL_TRADE_SOURCE_KEY)
    program = profile.by_key[INTERNATIONAL_TRADE_PROGRAM_KEY]
    if (
        profile.registry_id != registry.registry_id
        or program.source_key != INTERNATIONAL_TRADE_SOURCE_KEY
        or program.archive_uri != INTERNATIONAL_TRADE_INDEX_URI
        or source.archive_uri != INTERNATIONAL_TRADE_INDEX_URI
        or source.parser_id != "official.census-ft900.v1"
    ):
        raise ValueError("international-trade registry/profile binding differs")
    required_uris = {
        INTERNATIONAL_TRADE_PREDECESSOR_URI,
        *(item.artifact_uri for item in release_index.releases),
    }
    if set(snapshots_by_uri) != required_uris:
        missing = sorted(required_uris - set(snapshots_by_uri))
        unexpected = sorted(set(snapshots_by_uri) - required_uris)
        raise ValueError(
            "international-trade retained corpus differs: "
            f"missing={missing[:3]}, unexpected={unexpected[:3]}"
        )
    predecessor_snapshot = snapshots_by_uri[INTERNATIONAL_TRADE_PREDECESSOR_URI]
    _validate_release_snapshot(
        predecessor_snapshot, uri=INTERNATIONAL_TRADE_PREDECESSOR_URI
    )
    predecessor_text = _source_text(predecessor_snapshot.content)
    predecessor_match = _BALANCE_RE.search(
        re.sub(r"(?<=\d)\s+(?=\d)", "", predecessor_text)
    )
    if predecessor_match is None:
        raise ValueError(
            "international-trade predecessor value is not parseable"
        )
    predecessor_lexical = predecessor_match.group("actual")
    predecessor = _artifact(predecessor_snapshot)
    known: dict[str, tuple[float, str, str]] = {
        INTERNATIONAL_TRADE_PREDECESSOR_REFERENCE_PERIOD: (
            float(predecessor_lexical),
            predecessor_lexical,
            predecessor.artifact_uri,
        )
    }
    publications: list[CensusInternationalTradePublicationV1] = []
    for entry in release_index.releases:
        snapshot = snapshots_by_uri[entry.artifact_uri]
        _validate_release_snapshot(snapshot, uri=entry.artifact_uri)
        special = _SPECIAL_ROWS.get(snapshot.content_sha256)
        if special is not None:
            if special.reference_period != entry.reference_period:
                raise ValueError(
                    "international-trade special artifact is misbound"
                )
            actual_lexical = special.actual_lexical
            revised_lexical = special.revised_previous_lexical
            metadata = _special_metadata(special)
            parser_era = "hash-bound-pdf"
            annual_context = entry.reference_period.endswith("-04")
            disruption_notice = False
        else:
            if (
                entry.reference_period
                in INTERNATIONAL_TRADE_SPECIAL_REFERENCE_PERIODS
            ):
                raise ValueError(
                    "international-trade special artifact hash differs"
                )
            source_text = _source_text(snapshot.content)
            actual_lexical, revised_lexical = _source_values(
                source_text, reference_period=entry.reference_period
            )
            metadata = _release_metadata(
                source_text, reference_period=entry.reference_period
            )
            parser_era = "native-pdf-standard"
            annual_context = entry.reference_period.endswith("-04")
            if (
                annual_context
                and _ANNUAL_REVISION_RE.search(source_text) is None
            ):
                raise ValueError(
                    "international-trade annual revision notice is absent"
                )
            disruption_notice = _DISRUPTION_RE.search(source_text) is not None
        previous_period = _previous_month(entry.reference_period)
        previous = known.get(previous_period)
        if previous is None:
            raise ValueError(
                "international-trade previous publication is absent"
            )
        measure = CensusInternationalTradeMeasureV1(
            measure_key=INTERNATIONAL_TRADE_MEASURE_KEY,
            reference_period=entry.reference_period,
            actual_deficit_billions=float(actual_lexical),
            actual_lexical=actual_lexical,
            previous_reference_period=previous_period,
            revised_previous_deficit_billions=float(revised_lexical),
            revised_previous_lexical=revised_lexical,
            previous_as_known_reference_period=previous_period,
            previous_as_known_deficit_billions=previous[0],
            previous_as_known_lexical=previous[1],
            previous_artifact_uri=previous[2],
            annual_revision_context=annual_context,
            comparison_basis=(
                "concurrent-annual-revision"
                if annual_context
                else "prior-monthly-publication"
            ),
        )
        release_date, lexical, released_ns, zone, basis, consistent, header = (
            metadata
        )
        artifact = _artifact(snapshot)
        publications.append(
            CensusInternationalTradePublicationV1(
                index_entry_id=entry.entry_id,
                reference_period=entry.reference_period,
                release_date=release_date,
                released_lexical=lexical,
                released_at_ns=released_ns,
                reported_zone=zone,
                zone_basis=basis,
                zone_consistent=consistent,
                artifact=artifact,
                parser_era=parser_era,
                source_header_lexical=header,
                annual_revision_context=annual_context,
                federal_disruption_notice=disruption_notice,
                measure=measure,
            )
        )
        known[entry.reference_period] = (
            measure.actual_deficit_billions,
            measure.actual_lexical,
            artifact.artifact_uri,
        )
    return CensusInternationalTradeArchiveManifestV1(
        registry_id=registry.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        predecessor=predecessor,
        predecessor_actual_deficit_billions=float(predecessor_lexical),
        predecessor_actual_lexical=predecessor_lexical,
        publications=tuple(publications),
        raw_artifact_count=len(publications) + 2,
        unique_content_sha256_count=len(
            {
                release_index.content_sha256,
                predecessor.content_sha256,
                *(item.artifact.content_sha256 for item in publications),
            }
        ),
        total_content_bytes=(
            release_index.content_length
            + predecessor.content_length
            + sum(item.artifact.content_length for item in publications)
        ),
        measure_occurrence_count=len(publications),
        previous_as_known_measure_count=len(publications),
        changed_revision_count=sum(
            item.measure.previous_was_revised for item in publications
        ),
        corrected_link_count=sum(
            item.link_corrected for item in release_index.releases
        ),
        native_pdf_publication_count=sum(
            item.parser_era == "native-pdf-standard" for item in publications
        ),
        hash_bound_publication_count=sum(
            item.parser_era == "hash-bound-pdf" for item in publications
        ),
        inferred_zone_count=sum(
            item.reported_zone is None for item in publications
        ),
        source_zone_token_count=sum(
            item.reported_zone is not None for item in publications
        ),
        zone_mismatch_count=sum(
            item.zone_consistent is False for item in publications
        ),
        annual_revision_count=sum(
            item.annual_revision_context for item in publications
        ),
        federal_disruption_notice_count=sum(
            item.federal_disruption_notice for item in publications
        ),
        exact_minute_count=len(publications),
    )


def replay_census_international_trade_archive(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    index_snapshot: OfficialRawSnapshotV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    expected: CensusInternationalTradeArchiveManifestV1,
) -> CensusInternationalTradeArchiveManifestV1:
    """Recompute and compare the complete retained FT-900 corpus."""
    if not isinstance(expected, CensusInternationalTradeArchiveManifestV1):
        raise TypeError("international-trade replay requires a v1 manifest")
    release_index = parse_census_international_trade_release_index(
        index_snapshot, as_of_date=expected.release_index.as_of_date
    )
    rebuilt = build_census_international_trade_archive_manifest(
        registry, profile, release_index, snapshots_by_uri
    )
    if rebuilt != expected:
        raise ValueError("international-trade retained-corpus replay differs")
    return rebuilt


def census_international_trade_coverage_from_manifest(
    manifest: CensusInternationalTradeArchiveManifestV1,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete monthly coverage for the canonical U.S. profile."""
    if not isinstance(manifest, CensusInternationalTradeArchiveManifestV1):
        raise TypeError("international-trade coverage requires a v1 manifest")
    count = len(manifest.publications)
    return UnitedStatesProgramCoverageV1(
        program_key=INTERNATIONAL_TRADE_PROGRAM_KEY,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=manifest.release_index.as_of_date,
        expected_occurrence_count=count,
        schedule_count=count,
        initial_actual_count=count,
        previous_as_known_count=count,
        revision_count=manifest.changed_revision_count,
        exact_minute_count=count,
        forecast_count=0,
        artifact_sha256s=tuple(
            sorted(
                {
                    manifest.release_index.content_sha256,
                    manifest.predecessor.content_sha256,
                    *(
                        item.artifact.content_sha256
                        for item in manifest.publications
                    ),
                }
            )
        ),
        gap_reasons=(),
        notes=(
            "Six wrong-target index links use bounded surviving Census PDFs.",
            "The source spelling Ferbruary 2019 remains explicit in the retained index.",
            "Annual revisions retain the preceding initial vintage and the concurrently revised value.",
            "Legacy reports without a zone token use the producer's America/New_York timezone.",
            "No historical event-level consensus is manufactured from another survey.",
        ),
    )


def packaged_census_international_trade_archive_manifest_path() -> Path:
    """Return the packaged FT-900 archive-manifest path."""
    return (
        Path(__file__).with_name("assets")
        / "us_international_trade_archive_v1.json"
    )


def packaged_census_international_trade_index_path() -> Path:
    """Return the packaged base64 FT-900 index-envelope path."""
    return (
        Path(__file__).with_name("assets")
        / "us_international_trade_index_v1.json"
    )


def load_packaged_census_international_trade_archive_manifest() -> (
    CensusInternationalTradeArchiveManifestV1
):
    """Load and validate the packaged FT-900 archive manifest."""
    return CensusInternationalTradeArchiveManifestV1.from_json(
        packaged_census_international_trade_archive_manifest_path().read_text(
            encoding="utf-8"
        )
    )


def load_packaged_census_international_trade_index_snapshot() -> (
    OfficialRawSnapshotV1
):
    """Restore the exact packaged FT-900 index snapshot."""
    try:
        payload = json.loads(
            packaged_census_international_trade_index_path().read_text(
                encoding="utf-8"
            )
        )
    except json.JSONDecodeError as exc:
        raise ValueError(
            "packaged international-trade index envelope is invalid JSON"
        ) from exc
    data = _mapping(payload, "international-trade index envelope")
    if (
        data.get("schema_version")
        != INTERNATIONAL_TRADE_INDEX_ENVELOPE_SCHEMA_VERSION
    ):
        raise ValueError(
            "unsupported international-trade index-envelope schema"
        )
    encoded = data.get("content_base64")
    if (
        not isinstance(encoded, str)
        or not encoded
        or len(encoded) > (MAX_INTERNATIONAL_TRADE_INDEX_BYTES * 4 // 3) + 4
    ):
        raise ValueError("international-trade index content_base64 is invalid")
    try:
        content = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError(
            "international-trade index is not canonical base64"
        ) from exc
    snapshot = OfficialRawSnapshotV1.restore(
        _mapping(data.get("snapshot"), "international-trade index snapshot"),
        content,
    )
    if snapshot.request.uri != INTERNATIONAL_TRADE_INDEX_URI:
        raise ValueError("packaged international-trade index URI differs")
    return snapshot


def load_packaged_census_international_trade_index() -> (
    CensusInternationalTradeReleaseIndexV1
):
    """Replay the packaged index bytes at the manifest as-of date."""
    manifest = load_packaged_census_international_trade_archive_manifest()
    return parse_census_international_trade_release_index(
        load_packaged_census_international_trade_index_snapshot(),
        as_of_date=manifest.release_index.as_of_date,
    )


__all__ = [
    "INTERNATIONAL_TRADE_CORRECTED_PERIODS",
    "INTERNATIONAL_TRADE_FIRST_REFERENCE_PERIOD",
    "INTERNATIONAL_TRADE_INDEX_URI",
    "INTERNATIONAL_TRADE_MEASURE_KEY",
    "INTERNATIONAL_TRADE_PREDECESSOR_REFERENCE_PERIOD",
    "INTERNATIONAL_TRADE_PREDECESSOR_URI",
    "INTERNATIONAL_TRADE_PROGRAM_KEY",
    "INTERNATIONAL_TRADE_RELEASE_SCHEDULE_URI",
    "INTERNATIONAL_TRADE_RETIRED_INDEX_URI",
    "INTERNATIONAL_TRADE_SOURCE_KEY",
    "INTERNATIONAL_TRADE_SPECIAL_REFERENCE_PERIODS",
    "CensusInternationalTradeArchiveManifestV1",
    "CensusInternationalTradeArtifactV1",
    "CensusInternationalTradeMeasureV1",
    "CensusInternationalTradePublicationV1",
    "CensusInternationalTradeReleaseIndexEntryV1",
    "CensusInternationalTradeReleaseIndexV1",
    "build_census_international_trade_archive_manifest",
    "build_census_international_trade_archive_requests",
    "build_census_international_trade_index_request",
    "census_international_trade_coverage_from_manifest",
    "load_packaged_census_international_trade_archive_manifest",
    "load_packaged_census_international_trade_index",
    "load_packaged_census_international_trade_index_snapshot",
    "packaged_census_international_trade_archive_manifest_path",
    "packaged_census_international_trade_index_path",
    "parse_census_international_trade_release_index",
    "replay_census_international_trade_archive",
]
