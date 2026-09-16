"""Deterministic qualification of the Federal Reserve H.6 archive.

The H.6 release was weekly through February 11, 2021 and became monthly on
February 23, 2021.  This module preserves that frequency break, reconstructs
the seasonally adjusted M2 level published by every release, and keeps the
previous-as-known estimate distinct from the value shown after revision.

The compact packaged manifest contains hashes and normalized triplets.  Exact
release HTML/PDF bytes remain in a caller-retained directory and can be
replayed without network access.
"""

from __future__ import annotations

import base64
import calendar
import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import Enum
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, cast

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

H6_SOURCE_KEY: Final = "us.frb.money-stock"
H6_PARSER_ID: Final = "official.federal-reserve-h6.v1"
H6_PROGRAM_KEY: Final = "us.frb.money-stock"
H6_INDEX_URI: Final = "https://www.federalreserve.gov/releases/h6/"
H6_RELEASE_DATES_URI: Final = H6_INDEX_URI + "releaseDates.json"
H6_RELEASE_URI_TEMPLATE: Final = H6_INDEX_URI + "{release_date}/"
H6_TEXT_URI_TEMPLATE: Final = H6_INDEX_URI + "{release_date}/h6.txt"
H6_PDF_URI_TEMPLATE: Final = H6_INDEX_URI + "{release_date}/h6.pdf"
H6_PREDECESSOR_RELEASE_DATE: Final = "1999-12-30"
H6_FIRST_RELEASE_DATE: Final = "2000-01-06"
H6_FINAL_WEEKLY_RELEASE_DATE: Final = "2021-02-11"
H6_FIRST_MONTHLY_RELEASE_DATE: Final = "2021-02-23"
H6_FIRST_PDF_RELEASE_DATE: Final = "2000-03-02"
H6_FINAL_PDF_RELEASE_DATE: Final = "2021-02-11"
H6_LATEST_PACKAGED_RELEASE_DATE: Final = "2026-08-25"
H6_SOURCE_TIMEZONE: Final = "America/New_York"

H6_ARTIFACT_SCHEMA_VERSION: Final = "histdatacom.h6-artifact.v1"
H6_INDEX_ENTRY_SCHEMA_VERSION: Final = "histdatacom.h6-index-entry.v1"
H6_RELEASE_INDEX_SCHEMA_VERSION: Final = "histdatacom.h6-release-index.v1"
H6_PUBLICATION_SCHEMA_VERSION: Final = "histdatacom.h6-publication.v1"
H6_ARCHIVE_MANIFEST_SCHEMA_VERSION: Final = "histdatacom.h6-archive-manifest.v1"
H6_INDEX_PAGES_SCHEMA_VERSION: Final = "histdatacom.h6-index-pages.v1"

MAX_H6_RELEASES: Final = 2_048
MAX_H6_ARTIFACT_BYTES: Final = 4 * 1024 * 1024
MAX_H6_TOTAL_BYTES: Final = 256 * 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_NUMBER_RE = re.compile(r"-?\d[\d,]*\.\d")
_FULL_WEEK_RE = re.compile(
    r"^\s*(?P<year>\d{4})-(?P<month>[A-Za-z]+)\.?\s+" r"(?P<day>\d{1,2})p?\s*"
)
_MONTH_WEEK_RE = re.compile(
    r"^\s*(?P<month>[A-Za-z]+)\.?\s+(?P<day>\d{1,2})p?\s*"
)
_DAY_WEEK_RE = re.compile(r"^\s*(?P<day>\d{1,2})p?\s*")
_FULL_MONTH_RE = re.compile(
    r"^\s*(?P<year>\d{4})-(?P<month>[A-Za-z]+)\.?\s*p?\s*"
)
_MONTH_ONLY_RE = re.compile(r"^\s*(?P<month>[A-Za-z]+)\.?\s*p?\s*")
_SEMANTIC_WEEK_RE = re.compile(
    r"^(?P<month>[A-Za-z]+)\.?\s+(?P<day>\d{1,2}),\s+" r"(?P<year>\d{4})$"
)
_SEMANTIC_MONTH_RE = re.compile(r"^(?P<month>[A-Za-z]+)\.?\s+(?P<year>\d{4})$")
_RELEASE_DATE_RE = re.compile(
    r"\bRelease Date:\s*([A-Z][a-z]+\s+\d{1,2},\s+(?:19|20)\d{2})\b"
)
_WRITTEN_DATE_RE = re.compile(
    r"^(?P<month>[A-Z][a-z]+)\s+(?P<day>\d{1,2}),\s+"
    r"(?P<year>(?:19|20)\d{2})$"
)
_TEXT_TABLE_DATE_RE = re.compile(
    r"\bTable 1[^\r\n]*(?:\r?\n)+\s*"
    r"(?P<date>[A-Z][a-z]+\s+\d{1,2},\s+20\d{2})\b"
)
_EXACT_TIME_RE = re.compile(
    r"\bFor release at\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*"
    r"p\.?m\.?\s+Eastern Time\s+"
    r"(?P<date>[A-Z][a-z]+\s+\d{1,2},\s+20\d{2})\b",
    re.IGNORECASE,
)
_INDEX_DATE_CORRECTIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "20050305": "20050303",
        "20130405": "20130404",
        "20161118": "20161117",
        "20171123": "20171124",
    }
)
_INDEX_ARTIFACT_DATES: Final[Mapping[str, str]] = MappingProxyType(
    {"20130405": "20130404"}
)
_INDEX_CORRECTION_NOTES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "20050305": (
            "The JSON ledger says March 5, 2005; the official page and PDF "
            "in that directory both author March 3."
        ),
        "20130405": (
            "The JSON ledger says April 5, 2013; that directory is absent, "
            "while the surviving official page and PDF both author April 4."
        ),
        "20161118": (
            "The JSON ledger says November 18, 2016; the official page and "
            "PDF in that directory both author November 17."
        ),
        "20171123": (
            "The JSON ledger says November 23, 2017; the official page and "
            "PDF in that directory both author November 24."
        ),
    }
)
_TEXT_RELEASE_DATES: Final = frozenset({"2002-06-13"})
_TIMING_PDF_EXCLUDED_RELEASE_DATES: Final = frozenset(
    {
        "2001-02-08",
        "2001-09-27",
        "2001-11-15",
        "2002-02-14",
        "2002-04-11",
        "2002-05-02",
        "2004-01-29",
        "2004-03-04",
        "2005-02-03",
        "2005-11-10",
        "2006-01-26",
        "2006-03-09",
        "2006-03-16",
        "2006-03-23",
        "2006-03-30",
        "2007-01-18",
        "2009-03-12",
        "2009-03-26",
        "2019-04-18",
    }
)
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


class H6PublicationFrequency(str, Enum):
    """Native frequency of one H.6 publication occurrence."""

    WEEKLY = "weekly"
    MONTHLY = "monthly"


def _required_text(value: object, name: str) -> str:
    text = str(value).strip()
    if not text or len(text) > 4096:
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


def _bounded_count(value: object, name: str, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside its bound")
    return value


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be numeric")
    try:
        result = float(cast(Any, value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _lexical_number(value: object, name: str) -> tuple[str, float]:
    text = _required_text(value, name).replace(",", "")
    if re.fullmatch(r"\d+\.\d", text) is None:
        raise ValueError(f"{name} must retain one decimal place")
    result = _finite(text, name)
    if f"{result:.1f}" != text:
        raise ValueError(f"{name} is not canonical")
    return text, result


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _month_number(value: str) -> int:
    try:
        return _MONTH_NUMBER[value.lower().rstrip(".")]
    except KeyError as exc:
        raise ValueError("unsupported H.6 month name") from exc


def _optional_month_number(value: str) -> int | None:
    return _MONTH_NUMBER.get(value.lower().rstrip("."))


def _written_date(value: str) -> str:
    match = _WRITTEN_DATE_RE.fullmatch(value)
    if match is None:
        raise ValueError("H.6 authored release date is malformed")
    return date(
        int(match.group("year")),
        _month_number(match.group("month")),
        int(match.group("day")),
    ).isoformat()


def _frequency(release_date: str) -> H6PublicationFrequency:
    return (
        H6PublicationFrequency.WEEKLY
        if release_date <= H6_FINAL_WEEKLY_RELEASE_DATE
        else H6PublicationFrequency.MONTHLY
    )


def _release_uri(release_date: str) -> str:
    return H6_RELEASE_URI_TEMPLATE.format(
        release_date=release_date.replace("-", "")
    )


def _text_uri(artifact_date: str) -> str:
    return H6_TEXT_URI_TEMPLATE.format(
        release_date=artifact_date.replace("-", "")
    )


def _pdf_uri(release_date: str) -> str:
    return H6_PDF_URI_TEMPLATE.format(
        release_date=release_date.replace("-", "")
    )


def _has_pdf(release_date: str) -> bool:
    return (
        H6_FIRST_PDF_RELEASE_DATE <= release_date <= H6_FINAL_PDF_RELEASE_DATE
        and release_date not in _TIMING_PDF_EXCLUDED_RELEASE_DATES
    )


@dataclass(frozen=True, slots=True)
class FederalReserveH6ArtifactV1:
    """One exact H.6 index, release, or timing artifact."""

    role: str
    source_uri: str
    source_format: str
    content_sha256: str
    content_length: int
    artifact_id: str = ""
    schema_version: str = H6_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != H6_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported H.6 artifact schema")
        role = _required_text(self.role, "role")
        uri = _required_text(self.source_uri, "source_uri")
        if not uri.startswith(H6_INDEX_URI):
            raise ValueError("H.6 artifact must use the official release host")
        source_format = _required_text(self.source_format, "source_format")
        if source_format not in {"html", "json", "pdf", "text"}:
            raise ValueError("unsupported H.6 artifact format")
        digest = _sha256(self.content_sha256, "content_sha256")
        if not 1 <= self.content_length <= MAX_H6_ARTIFACT_BYTES:
            raise ValueError("H.6 artifact length is outside its bound")
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "source_uri", uri)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "content_sha256", digest)
        expected = _stable_id("h6-artifact", self.identity_payload())
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("H.6 artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role,
            "source_uri": self.source_uri,
            "source_format": self.source_format,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FederalReserveH6ArtifactV1:
        return cls(
            role=str(data.get("role", "")),
            source_uri=str(data.get("source_uri", "")),
            source_format=str(data.get("source_format", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveH6IndexEntryV1:
    """One release enumerated by the official release-dates ledger."""

    indexed_date: str
    release_date: str
    frequency: H6PublicationFrequency
    release_uri: str
    release_format: OfficialSourceFormat
    timing_pdf_uri: str | None
    annual_seasonal_review: bool
    first_monthly_release: bool
    limitations: tuple[str, ...]
    entry_id: str = ""
    schema_version: str = H6_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != H6_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported H.6 index-entry schema")
        indexed = _iso_date(self.indexed_date, "indexed_date")
        release = _iso_date(self.release_date, "release_date")
        frequency = H6PublicationFrequency(self.frequency)
        if frequency is not _frequency(release):
            raise ValueError("H.6 entry frequency differs from release era")
        indexed_raw = indexed.replace("-", "")
        artifact_raw = _INDEX_ARTIFACT_DATES.get(indexed_raw, indexed_raw)
        artifact_date = date(
            int(artifact_raw[:4]),
            int(artifact_raw[4:6]),
            int(artifact_raw[6:]),
        ).isoformat()
        release_format = OfficialSourceFormat.from_value(self.release_format)
        expected_format = (
            OfficialSourceFormat.TEXT
            if release in _TEXT_RELEASE_DATES
            else OfficialSourceFormat.HTML
        )
        if release_format is not expected_format:
            raise ValueError("H.6 release format differs from archive evidence")
        release_uri = _required_text(self.release_uri, "release_uri")
        expected_uri = (
            _text_uri(artifact_date)
            if release_format is OfficialSourceFormat.TEXT
            else _release_uri(artifact_date)
        )
        if release_uri != expected_uri:
            raise ValueError("H.6 release URI differs from release date")
        pdf_uri = _optional_text(self.timing_pdf_uri, "timing_pdf_uri")
        if (pdf_uri is not None) != _has_pdf(release):
            raise ValueError("H.6 timing-PDF inventory differs")
        if pdf_uri is not None and pdf_uri != _pdf_uri(artifact_date):
            raise ValueError("H.6 timing-PDF URI differs")
        if not isinstance(self.annual_seasonal_review, bool) or not isinstance(
            self.first_monthly_release, bool
        ):
            raise TypeError("H.6 index flags must be boolean")
        if self.first_monthly_release != (
            release == H6_FIRST_MONTHLY_RELEASE_DATE
        ):
            raise ValueError("H.6 first-monthly marker differs")
        expected_indexed = _INDEX_DATE_CORRECTIONS.get(indexed_raw, indexed_raw)
        if expected_indexed != release.replace("-", ""):
            raise ValueError("H.6 indexed and authored release dates differ")
        limitations = tuple(
            _required_text(item, "limitation") for item in self.limitations
        )
        object.__setattr__(self, "indexed_date", indexed)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "frequency", frequency)
        object.__setattr__(self, "release_uri", release_uri)
        object.__setattr__(self, "release_format", release_format)
        object.__setattr__(self, "timing_pdf_uri", pdf_uri)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id("h6-index-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("H.6 index-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "indexed_date": self.indexed_date,
            "release_date": self.release_date,
            "frequency": self.frequency.value,
            "release_uri": self.release_uri,
            "release_format": self.release_format.value,
            "timing_pdf_uri": self.timing_pdf_uri,
            "annual_seasonal_review": self.annual_seasonal_review,
            "first_monthly_release": self.first_monthly_release,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FederalReserveH6IndexEntryV1:
        return cls(
            indexed_date=str(data.get("indexed_date", "")),
            release_date=str(data.get("release_date", "")),
            frequency=H6PublicationFrequency(str(data.get("frequency", ""))),
            release_uri=str(data.get("release_uri", "")),
            release_format=OfficialSourceFormat.from_value(
                str(data.get("release_format", ""))
            ),
            timing_pdf_uri=(
                None
                if data.get("timing_pdf_uri") is None
                else str(data.get("timing_pdf_uri"))
            ),
            annual_seasonal_review=cast(
                bool, data.get("annual_seasonal_review")
            ),
            first_monthly_release=cast(bool, data.get("first_monthly_release")),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveH6ReleaseIndexV1:
    """Complete official H.6 occurrence inventory with one predecessor."""

    as_of_date: str
    index_page: FederalReserveH6ArtifactV1
    release_dates: FederalReserveH6ArtifactV1
    entries: tuple[FederalReserveH6IndexEntryV1, ...]
    index_id: str = ""
    schema_version: str = H6_RELEASE_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != H6_RELEASE_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported H.6 release-index schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        if self.index_page.role != "release-index-html" or (
            self.release_dates.role != "release-dates-json"
        ):
            raise ValueError("H.6 release-index artifacts differ")
        entries = tuple(
            sorted(self.entries, key=lambda item: item.release_date)
        )
        if not 2 <= len(entries) <= MAX_H6_RELEASES:
            raise ValueError("H.6 release inventory is outside its bound")
        dates = tuple(item.release_date for item in entries)
        if dates[0] != H6_PREDECESSOR_RELEASE_DATE:
            raise ValueError("H.6 predecessor release differs")
        if dates[1] != H6_FIRST_RELEASE_DATE or dates[-1] > as_of:
            raise ValueError("H.6 in-scope release boundary differs")
        if len(set(dates)) != len(dates):
            raise ValueError("H.6 release inventory repeats a date")
        monthly = tuple(
            item
            for item in entries
            if item.frequency is H6PublicationFrequency.MONTHLY
        )
        if (
            not monthly
            or monthly[0].release_date != H6_FIRST_MONTHLY_RELEASE_DATE
        ):
            raise ValueError("H.6 monthly transition differs")
        if sum(item.first_monthly_release for item in entries) != 1:
            raise ValueError("H.6 first-monthly marker count differs")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id("h6-release-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("H.6 release-index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_release_date(self) -> Mapping[str, FederalReserveH6IndexEntryV1]:
        return MappingProxyType(
            {item.release_date: item for item in self.entries}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "index_page": self.index_page.to_dict(),
            "release_dates": self.release_dates.to_dict(),
            "entries": [item.to_dict() for item in self.entries],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FederalReserveH6ReleaseIndexV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            index_page=FederalReserveH6ArtifactV1.from_dict(
                _mapping(data.get("index_page"), "index_page")
            ),
            release_dates=FederalReserveH6ArtifactV1.from_dict(
                _mapping(data.get("release_dates"), "release_dates")
            ),
            entries=tuple(
                FederalReserveH6IndexEntryV1.from_dict(
                    _mapping(item, "H.6 index entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveH6PublicationV1:
    """One occurrence-specific seasonally adjusted M2 triplet."""

    release_date: str
    frequency: H6PublicationFrequency
    reference_period: str
    previous_reference_period: str
    release_time_local: str
    time_precision: EconomicTimePrecision
    released_at_ns: int | None
    actual_lexical: str
    actual_value: float
    previous_as_known_lexical: str
    previous_as_known_value: float
    revised_previous_lexical: str
    revised_previous_value: float
    annual_seasonal_review: bool
    first_monthly_release: bool
    parser_era: str
    release_artifact: FederalReserveH6ArtifactV1
    timing_pdf: FederalReserveH6ArtifactV1 | None
    release_time_locator: str
    value_locator: str
    revision_locator: str
    publication_id: str = ""
    schema_version: str = H6_PUBLICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != H6_PUBLICATION_SCHEMA_VERSION:
            raise ValueError("unsupported H.6 publication schema")
        release = _iso_date(self.release_date, "release_date")
        frequency = H6PublicationFrequency(self.frequency)
        if frequency is not _frequency(release):
            raise ValueError("H.6 publication frequency differs")
        reference = _required_text(self.reference_period, "reference_period")
        previous_reference = _required_text(
            self.previous_reference_period, "previous_reference_period"
        )
        period_pattern = (
            r"\d{4}-\d{2}-\d{2}"
            if frequency is H6PublicationFrequency.WEEKLY
            else r"\d{4}-\d{2}"
        )
        if (
            re.fullmatch(period_pattern, reference) is None
            or re.fullmatch(period_pattern, previous_reference) is None
        ):
            raise ValueError("H.6 reference-period shape differs")
        if previous_reference >= reference:
            raise ValueError("H.6 previous reference period is not earlier")
        local = _required_text(self.release_time_local, "release_time_local")
        if _TIME_RE.fullmatch(local) is None:
            raise ValueError("H.6 release time must be HH:MM")
        expected_local = (
            "16:30" if frequency is H6PublicationFrequency.WEEKLY else "13:00"
        )
        if local != expected_local:
            raise ValueError("H.6 release clock differs from its era")
        precision = EconomicTimePrecision.from_value(self.time_precision)
        if precision is EconomicTimePrecision.EXACT_MINUTE:
            if self.released_at_ns is None:
                raise ValueError("exact H.6 publication requires an instant")
            expected_ns = normalize_official_source_timestamp(
                f"{release}T{local}:00",
                H6_SOURCE_TIMEZONE,
                EconomicTimePrecision.EXACT_MINUTE,
            ).utc_ns
            if self.released_at_ns != expected_ns:
                raise ValueError("H.6 release instant differs")
        elif precision is EconomicTimePrecision.SCHEDULED_ONLY:
            if self.released_at_ns is not None:
                raise ValueError(
                    "scheduled-only H.6 time cannot invent an instant"
                )
        else:
            raise ValueError("unsupported H.6 time precision")
        values = (
            ("actual", self.actual_lexical, self.actual_value),
            (
                "previous_as_known",
                self.previous_as_known_lexical,
                self.previous_as_known_value,
            ),
            (
                "revised_previous",
                self.revised_previous_lexical,
                self.revised_previous_value,
            ),
        )
        for name, lexical, numeric in values:
            normalized, parsed = _lexical_number(lexical, name)
            if normalized != lexical or parsed != numeric:
                raise ValueError(f"H.6 {name} lexical/numeric values differ")
        if not isinstance(self.annual_seasonal_review, bool) or not isinstance(
            self.first_monthly_release, bool
        ):
            raise TypeError("H.6 publication flags must be boolean")
        if self.first_monthly_release != (
            release == H6_FIRST_MONTHLY_RELEASE_DATE
        ):
            raise ValueError("H.6 publication first-monthly flag differs")
        era = _required_text(self.parser_era, "parser_era")
        if era not in {
            "preformatted-m3-weekly",
            "preformatted-m2-weekly",
            "legacy-table-weekly",
            "modern-table-weekly",
            "modern-table-monthly",
        }:
            raise ValueError("unsupported H.6 parser era")
        if self.release_artifact.role != "release-artifact":
            raise ValueError("H.6 publication release artifact differs")
        timing = self.timing_pdf
        if timing is not None and timing.role != "timing-pdf":
            raise ValueError("H.6 timing artifact differs")
        if (timing is not None) != _has_pdf(release):
            raise ValueError("H.6 publication timing-PDF differs")
        for name in (
            "release_time_locator",
            "value_locator",
            "revision_locator",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "frequency", frequency)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(
            self, "previous_reference_period", previous_reference
        )
        object.__setattr__(self, "release_time_local", local)
        object.__setattr__(self, "time_precision", precision)
        object.__setattr__(self, "parser_era", era)
        expected = _stable_id("h6-publication", self.identity_payload())
        if self.publication_id and self.publication_id != expected:
            raise ValueError("H.6 publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    @property
    def revision_delta(self) -> float:
        return round(
            self.revised_previous_value - self.previous_as_known_value, 1
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "release_date": self.release_date,
            "frequency": self.frequency.value,
            "reference_period": self.reference_period,
            "previous_reference_period": self.previous_reference_period,
            "release_time_local": self.release_time_local,
            "time_precision": self.time_precision.value,
            "released_at_ns": self.released_at_ns,
            "actual_lexical": self.actual_lexical,
            "actual_value": self.actual_value,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "previous_as_known_value": self.previous_as_known_value,
            "revised_previous_lexical": self.revised_previous_lexical,
            "revised_previous_value": self.revised_previous_value,
            "annual_seasonal_review": self.annual_seasonal_review,
            "first_monthly_release": self.first_monthly_release,
            "parser_era": self.parser_era,
            "release_artifact": self.release_artifact.to_dict(),
            "timing_pdf": (
                None if self.timing_pdf is None else self.timing_pdf.to_dict()
            ),
            "release_time_locator": self.release_time_locator,
            "value_locator": self.value_locator,
            "revision_locator": self.revision_locator,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "publication_id": self.publication_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FederalReserveH6PublicationV1:
        timing = data.get("timing_pdf")
        return cls(
            release_date=str(data.get("release_date", "")),
            frequency=H6PublicationFrequency(str(data.get("frequency", ""))),
            reference_period=str(data.get("reference_period", "")),
            previous_reference_period=str(
                data.get("previous_reference_period", "")
            ),
            release_time_local=str(data.get("release_time_local", "")),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            released_at_ns=cast(int | None, data.get("released_at_ns")),
            actual_lexical=str(data.get("actual_lexical", "")),
            actual_value=cast(float, data.get("actual_value")),
            previous_as_known_lexical=str(
                data.get("previous_as_known_lexical", "")
            ),
            previous_as_known_value=cast(
                float, data.get("previous_as_known_value")
            ),
            revised_previous_lexical=str(
                data.get("revised_previous_lexical", "")
            ),
            revised_previous_value=cast(
                float, data.get("revised_previous_value")
            ),
            annual_seasonal_review=cast(
                bool, data.get("annual_seasonal_review")
            ),
            first_monthly_release=cast(bool, data.get("first_monthly_release")),
            parser_era=str(data.get("parser_era", "")),
            release_artifact=FederalReserveH6ArtifactV1.from_dict(
                _mapping(data.get("release_artifact"), "release_artifact")
            ),
            timing_pdf=(
                None
                if timing is None
                else FederalReserveH6ArtifactV1.from_dict(
                    _mapping(timing, "timing_pdf")
                )
            ),
            release_time_locator=str(data.get("release_time_locator", "")),
            value_locator=str(data.get("value_locator", "")),
            revision_locator=str(data.get("revision_locator", "")),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class FederalReserveH6ArchiveManifestV1:
    """Complete content-addressed H.6 release and triplet manifest."""

    registry_id: str
    profile_id: str
    release_index: FederalReserveH6ReleaseIndexV1
    predecessor_artifact: FederalReserveH6ArtifactV1
    publications: tuple[FederalReserveH6PublicationV1, ...]
    raw_artifact_count: int
    total_content_bytes: int
    release_artifact_bytes: int
    timing_pdf_bytes: int
    weekly_count: int
    monthly_count: int
    exact_minute_count: int
    scheduled_only_count: int
    revision_occurrence_count: int
    manifest_id: str = ""
    schema_version: str = H6_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != H6_ARCHIVE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported H.6 archive-manifest schema")
        registry = _required_text(self.registry_id, "registry_id")
        profile = _required_text(self.profile_id, "profile_id")
        if not registry.startswith("official-source-registry:sha256:") or not (
            profile.startswith("us-backfill-profile:sha256:")
        ):
            raise ValueError("H.6 registry/profile identity is invalid")
        if self.predecessor_artifact.role != "predecessor-artifact":
            raise ValueError("H.6 predecessor artifact differs")
        publications = tuple(self.publications)
        expected_entries = self.release_index.entries[1:]
        if len(publications) != len(expected_entries):
            raise ValueError("H.6 publication inventory differs")
        for publication, entry in zip(
            publications, expected_entries, strict=True
        ):
            if (
                publication.release_date != entry.release_date
                or publication.frequency is not entry.frequency
                or publication.annual_seasonal_review
                != entry.annual_seasonal_review
                or publication.first_monthly_release
                != entry.first_monthly_release
                or publication.release_artifact.source_uri != entry.release_uri
                or (
                    None
                    if publication.timing_pdf is None
                    else publication.timing_pdf.source_uri
                )
                != entry.timing_pdf_uri
            ):
                raise ValueError("H.6 publication differs from index entry")
        if publications[0].release_date != H6_FIRST_RELEASE_DATE:
            raise ValueError("H.6 first publication differs")
        artifacts = (
            self.release_index.index_page,
            self.release_index.release_dates,
            self.predecessor_artifact,
            *(item.release_artifact for item in publications),
            *(
                item.timing_pdf
                for item in publications
                if item.timing_pdf is not None
            ),
        )
        if len({item.source_uri for item in artifacts}) != len(artifacts):
            raise ValueError("H.6 raw artifact inventory repeats a URI")
        expected_counts = (
            len(artifacts),
            sum(item.content_length for item in artifacts),
            self.predecessor_artifact.content_length
            + sum(
                item.release_artifact.content_length for item in publications
            ),
            sum(
                item.timing_pdf.content_length
                for item in publications
                if item.timing_pdf is not None
            ),
            sum(
                item.frequency is H6PublicationFrequency.WEEKLY
                for item in publications
            ),
            sum(
                item.frequency is H6PublicationFrequency.MONTHLY
                for item in publications
            ),
            sum(
                item.time_precision is EconomicTimePrecision.EXACT_MINUTE
                for item in publications
            ),
            sum(
                item.time_precision is EconomicTimePrecision.SCHEDULED_ONLY
                for item in publications
            ),
            sum(item.revision_delta != 0 for item in publications),
        )
        actual_counts = (
            self.raw_artifact_count,
            self.total_content_bytes,
            self.release_artifact_bytes,
            self.timing_pdf_bytes,
            self.weekly_count,
            self.monthly_count,
            self.exact_minute_count,
            self.scheduled_only_count,
            self.revision_occurrence_count,
        )
        if actual_counts != expected_counts:
            raise ValueError("H.6 manifest summary counts differ")
        if self.total_content_bytes > MAX_H6_TOTAL_BYTES:
            raise ValueError("H.6 retained corpus exceeds its byte bound")
        if self.weekly_count + self.monthly_count != len(publications):
            raise ValueError("H.6 frequency counts differ")
        if self.exact_minute_count + self.scheduled_only_count != len(
            publications
        ):
            raise ValueError("H.6 time-precision counts differ")
        object.__setattr__(self, "registry_id", registry)
        object.__setattr__(self, "profile_id", profile)
        object.__setattr__(self, "publications", publications)
        expected = _stable_id("h6-archive-manifest", self.identity_payload())
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("H.6 archive-manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def by_release_date(self) -> Mapping[str, FederalReserveH6PublicationV1]:
        return MappingProxyType(
            {item.release_date: item for item in self.publications}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "predecessor_artifact": self.predecessor_artifact.to_dict(),
            "publications": [item.to_dict() for item in self.publications],
            "raw_artifact_count": self.raw_artifact_count,
            "total_content_bytes": self.total_content_bytes,
            "release_artifact_bytes": self.release_artifact_bytes,
            "timing_pdf_bytes": self.timing_pdf_bytes,
            "weekly_count": self.weekly_count,
            "monthly_count": self.monthly_count,
            "exact_minute_count": self.exact_minute_count,
            "scheduled_only_count": self.scheduled_only_count,
            "revision_occurrence_count": self.revision_occurrence_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> FederalReserveH6ArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=FederalReserveH6ReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            predecessor_artifact=FederalReserveH6ArtifactV1.from_dict(
                _mapping(
                    data.get("predecessor_artifact"), "predecessor_artifact"
                )
            ),
            publications=tuple(
                FederalReserveH6PublicationV1.from_dict(
                    _mapping(item, "H.6 publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            release_artifact_bytes=cast(
                int, data.get("release_artifact_bytes")
            ),
            timing_pdf_bytes=cast(int, data.get("timing_pdf_bytes")),
            weekly_count=cast(int, data.get("weekly_count")),
            monthly_count=cast(int, data.get("monthly_count")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            scheduled_only_count=cast(int, data.get("scheduled_only_count")),
            revision_occurrence_count=cast(
                int, data.get("revision_occurrence_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> FederalReserveH6ArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("H.6 manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "H.6 manifest"))


class _H6HtmlParser(HTMLParser):
    """Collect preformatted text, selected tables, and visible metadata."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.pre_depth = 0
        self.pre_parts: list[str] = []
        self.visible_parts: list[str] = []
        self.table_id: str | None = None
        self.table_class: str | None = None
        self.rows: dict[str, list[tuple[str, ...]]] = {
            "t1tg1": [],
            "t2tg1": [],
        }
        self.row: list[str] | None = None
        self.cell: list[str] | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        lowered = tag.lower()
        attributes = dict(attrs)
        if lowered == "pre":
            self.pre_depth += 1
        if lowered == "table" and attributes.get("id") in self.rows:
            self.table_id = cast(str, attributes["id"])
            if self.table_id == "t1tg1":
                self.table_class = attributes.get("class")
        elif self.table_id is not None and lowered == "tr":
            self.row = []
        elif (
            self.table_id is not None
            and self.row is not None
            and lowered in {"th", "td"}
        ):
            self.cell = []
        elif self.cell is not None and lowered == "br":
            self.cell.append(" ")
        if lowered in {"br", "p", "div", "h1", "h2", "h3", "h4", "tr"}:
            self.visible_parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered == "pre":
            self.pre_depth -= 1
        if (
            self.table_id is not None
            and lowered in {"th", "td"}
            and self.cell is not None
            and self.row is not None
        ):
            self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        elif (
            self.table_id is not None
            and lowered == "tr"
            and self.row is not None
        ):
            self.rows[self.table_id].append(tuple(self.row))
            self.row = None
        elif self.table_id is not None and lowered == "table":
            self.table_id = None
        if lowered in {"p", "div", "h1", "h2", "h3", "h4", "tr"}:
            self.visible_parts.append(" ")

    def handle_data(self, data: str) -> None:
        self.visible_parts.append(data)
        if self.pre_depth:
            self.pre_parts.append(data)
        if self.cell is not None:
            self.cell.append(data)

    @property
    def visible_text(self) -> str:
        return " ".join("".join(self.visible_parts).split())


@dataclass(frozen=True, slots=True)
class _ParsedH6Release:
    release_date: str
    parser_era: str
    target_rows: tuple[tuple[str, str], ...]
    monthly_rows: tuple[tuple[str, str], ...]
    exact_time_from_release: bool


def _preformatted_weekly_rows(text: str) -> tuple[tuple[str, str], ...]:
    header = re.search(
        r"Period [Ee]nding.*?Seasonally adjusted", text, re.DOTALL
    )
    if header is None:
        raise ValueError("H.6 preformatted weekly table header is missing")
    tail = text[header.end() :]
    first = re.search(r"^\s*\d{4}-[A-Za-z]+\.?\s+\d", tail, re.MULTILINE)
    if first is None:
        raise ValueError("H.6 preformatted weekly table has no first row")
    marker_positions = [
        tail.find(marker, first.start() + 1)
        for marker in (
            "Not seasonally adjusted",
            "Percent change at seasonally adjusted annual rates",
            "Note: Special caution",
        )
    ]
    endings = [item for item in marker_positions if item >= 0]
    block = tail[: min(endings)] if endings else tail
    current_year: int | None = None
    current_month: int | None = None
    result: list[tuple[str, str]] = []
    for line in block.splitlines():
        match = _FULL_WEEK_RE.match(line)
        if match is not None:
            parsed_month = _optional_month_number(match.group("month"))
            if parsed_month is None:
                continue
            current_year = int(match.group("year"))
            current_month = parsed_month
            day = int(match.group("day"))
            end = match.end()
        else:
            match = _MONTH_WEEK_RE.match(line)
            if match is not None:
                parsed_month = _optional_month_number(match.group("month"))
                if parsed_month is None:
                    continue
                current_month = parsed_month
                day = int(match.group("day"))
                end = match.end()
            else:
                match = _DAY_WEEK_RE.match(line)
                if match is None:
                    continue
                day = int(match.group("day"))
                end = match.end()
        values = _NUMBER_RE.findall(line[end:])
        if current_year is None or current_month is None or len(values) < 6:
            continue
        reference = date(current_year, current_month, day).isoformat()
        lexical, _ = _lexical_number(values[5], "weekly M2")
        result.append((reference, lexical))
    return tuple(result)


def _preformatted_monthly_rows(
    text: str, *, separate_nonseasonal: bool
) -> tuple[tuple[str, str], ...]:
    terminator = (
        r"Not seasonally adjusted"
        if separate_nonseasonal
        else r"Percent change at seasonally adjusted annual rates"
    )
    match = re.search(
        rf"\bTable 1\b(?P<body>.*?){terminator}",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    if match is None:
        raise ValueError("H.6 preformatted monthly table is missing")
    current_year: int | None = None
    result: list[tuple[str, str]] = []
    for line in match.group("body").splitlines():
        row = _FULL_MONTH_RE.match(line)
        if row is not None:
            parsed_month = _optional_month_number(row.group("month"))
            if parsed_month is None:
                continue
            current_year = int(row.group("year"))
            month = parsed_month
            end = row.end()
        else:
            row = _MONTH_ONLY_RE.match(line)
            if row is None:
                continue
            parsed_month = _optional_month_number(row.group("month"))
            if parsed_month is None:
                continue
            month = parsed_month
            end = row.end()
        values = _NUMBER_RE.findall(line[end:])
        if current_year is None or len(values) < 2:
            continue
        lexical, _ = _lexical_number(values[1], "monthly M2")
        result.append((f"{current_year:04d}-{month:02d}", lexical))
    return tuple(result)


def _semantic_rows(
    rows: Sequence[tuple[str, ...]], *, weekly: bool
) -> tuple[tuple[str, str], ...]:
    result: list[tuple[str, str]] = []
    for row in rows:
        minimum = 7 if weekly else 3
        if len(row) < minimum:
            continue
        match = (
            _SEMANTIC_WEEK_RE.fullmatch(row[0])
            if weekly
            else _SEMANTIC_MONTH_RE.fullmatch(row[0])
        )
        if match is None:
            continue
        month = _optional_month_number(match.group("month"))
        if month is None:
            continue
        reference = f"{int(match.group('year')):04d}-{month:02d}"
        if weekly:
            reference += f"-{int(match.group('day')):02d}"
        lexical, _ = _lexical_number(row[6 if weekly else 2], "semantic M2")
        result.append((reference, lexical))
    return tuple(result)


def _authored_release_date(visible_text: str) -> str:
    match = _RELEASE_DATE_RE.search(visible_text)
    if match is None:
        raise ValueError("H.6 release page has no authored release date")
    return _written_date(match.group(1))


def _has_exact_release_time(visible_text: str, release_date: str) -> bool:
    match = _EXACT_TIME_RE.search(visible_text)
    if match is None:
        return False
    authored = _written_date(match.group("date"))
    expected = (
        "4:30" if release_date <= H6_FINAL_WEEKLY_RELEASE_DATE else "1:00"
    )
    return authored == release_date and (
        f"{int(match.group('hour'))}:{match.group('minute')}" == expected
    )


def _parse_release(
    content: bytes, entry: FederalReserveH6IndexEntryV1
) -> _ParsedH6Release:
    decoded = content.decode("utf-8-sig")
    parser: _H6HtmlParser | None
    if entry.release_format is OfficialSourceFormat.TEXT:
        parser = None
        match = _TEXT_TABLE_DATE_RE.search(decoded)
        if match is None:
            raise ValueError("H.6 text release has no authored table date")
        authored = _written_date(match.group("date"))
        preformatted: str | None = decoded
        visible_text = decoded
    else:
        parser = _H6HtmlParser()
        parser.feed(decoded)
        parser.close()
        authored = _authored_release_date(parser.visible_text)
        preformatted = "".join(parser.pre_parts) if parser.pre_parts else None
        visible_text = parser.visible_text
    if authored != entry.release_date:
        raise ValueError("H.6 page date differs from release index")
    if preformatted is not None:
        monthly = _preformatted_monthly_rows(
            preformatted,
            separate_nonseasonal=entry.release_date <= "2006-03-16",
        )
        target = (
            _preformatted_weekly_rows(preformatted)
            if entry.frequency is H6PublicationFrequency.WEEKLY
            else monthly
        )
        era = (
            "preformatted-m3-weekly"
            if entry.release_date <= "2006-03-16"
            else "preformatted-m2-weekly"
        )
    else:
        if parser is None:
            raise ValueError("H.6 semantic release parser is unavailable")
        monthly = _semantic_rows(parser.rows["t1tg1"], weekly=False)
        target = (
            _semantic_rows(parser.rows["t2tg1"], weekly=True)
            if entry.frequency is H6PublicationFrequency.WEEKLY
            else monthly
        )
        if entry.frequency is H6PublicationFrequency.MONTHLY:
            era = "modern-table-monthly"
        elif parser.table_class == "statistics":
            era = "legacy-table-weekly"
        elif parser.table_class == "pubtables":
            era = "modern-table-weekly"
        else:
            raise ValueError("unsupported H.6 semantic table era")
    if len(target) < 2 or len(monthly) < 2:
        raise ValueError("H.6 release lacks comparison rows")
    if len(dict(target)) != len(target) or len(dict(monthly)) != len(monthly):
        raise ValueError("H.6 release repeats a reference period")
    if tuple(item[0] for item in target) != tuple(
        sorted(item[0] for item in target)
    ):
        raise ValueError("H.6 target rows are not chronological")
    return _ParsedH6Release(
        release_date=authored,
        parser_era=era,
        target_rows=target,
        monthly_rows=monthly,
        exact_time_from_release=_has_exact_release_time(
            visible_text, entry.release_date
        ),
    )


def _pdf_text(content: bytes) -> str:
    if not content.startswith(b"%PDF-"):
        raise ValueError("H.6 timing artifact lacks a PDF signature")
    try:
        page = PdfReader(BytesIO(content), strict=True).pages[0]
        text = page.extract_text() or ""
    except Exception as exc:
        raise ValueError("H.6 timing PDF cannot be parsed") from exc
    normalized = " ".join(text.split())
    if not normalized:
        raise ValueError("H.6 timing PDF has no text")
    return normalized


def _validate_pdf_time(content: bytes, release_date: str) -> None:
    text = _pdf_text(content)
    match = re.search(
        r"For release at (?P<hour>\d{1,2}):(?P<minute>\d{2}) p\.?m\.?"
        r" (?:Eastern Time|E[DS]?T).*?"
        r"(?P<date>[A-Z][a-z]+ \d{1,2}, 20\d{2})",
        text,
        re.IGNORECASE,
    )
    if match is None:
        raise ValueError("H.6 timing PDF lacks authored clock/date")
    authored = _written_date(match.group("date"))
    expected = (
        "16:30" if release_date <= H6_FINAL_WEEKLY_RELEASE_DATE else "13:00"
    )
    hour = int(match.group("hour")) + 12
    observed = f"{hour:02d}:{match.group('minute')}"
    if authored != release_date or observed != expected:
        raise ValueError("H.6 timing PDF clock/date differs")


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    as_of_date: str,
) -> OfficialSourceRequestV1:
    source = registry.source(H6_SOURCE_KEY)
    required = {
        OfficialSourceFormat.HTML,
        OfficialSourceFormat.JSON,
        OfficialSourceFormat.PDF,
        OfficialSourceFormat.TEXT,
    }
    if source.parser_id != H6_PARSER_ID or not required <= set(source.formats):
        raise ValueError("H.6 source registry binding differs")
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=US_BACKFILL_START_DATE,
        window_end=as_of_date,
    )


def build_federal_reserve_h6_index_requests(
    registry: OfficialSourceRegistryV1, *, as_of_date: str
) -> tuple[OfficialSourceRequestV1, OfficialSourceRequestV1]:
    """Plan the official HTML landing page and JSON release ledger."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("H.6 requests require a v1 registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    return (
        _request(
            registry,
            H6_INDEX_URI,
            OfficialSourceFormat.HTML,
            as_of_date=as_of,
        ),
        _request(
            registry,
            H6_RELEASE_DATES_URI,
            OfficialSourceFormat.JSON,
            as_of_date=as_of,
        ),
    )


def _validate_snapshot(
    snapshot: OfficialRawSnapshotV1,
    *,
    uri: str,
    source_format: OfficialSourceFormat,
) -> None:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("H.6 evidence requires a v1 snapshot")
    if (
        snapshot.request.source_key != H6_SOURCE_KEY
        or snapshot.request.parser_id != H6_PARSER_ID
        or snapshot.request.uri != uri
        or snapshot.request.source_format is not source_format
        or snapshot.status_code != 200
        or not snapshot.content
        or len(snapshot.content) > MAX_H6_ARTIFACT_BYTES
    ):
        raise ValueError("H.6 official snapshot differs")


def _artifact(
    snapshot: OfficialRawSnapshotV1, role: str
) -> FederalReserveH6ArtifactV1:
    source_format = snapshot.request.source_format
    if (
        source_format is OfficialSourceFormat.PDF
        and not snapshot.content.startswith(b"%PDF-")
    ):
        raise ValueError("H.6 PDF snapshot lacks a signature")
    return FederalReserveH6ArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=source_format.value,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
    )


def parse_federal_reserve_h6_release_index(
    index_snapshots: Sequence[OfficialRawSnapshotV1], *, as_of_date: str
) -> FederalReserveH6ReleaseIndexV1:
    """Parse the official JSON ledger and bind its HTML landing page."""
    as_of = _iso_date(as_of_date, "as_of_date")
    snapshots = {item.request.uri: item for item in index_snapshots}
    if set(snapshots) != {H6_INDEX_URI, H6_RELEASE_DATES_URI}:
        raise ValueError("H.6 release-index snapshot inventory differs")
    html_snapshot = snapshots[H6_INDEX_URI]
    json_snapshot = snapshots[H6_RELEASE_DATES_URI]
    _validate_snapshot(
        html_snapshot,
        uri=H6_INDEX_URI,
        source_format=OfficialSourceFormat.HTML,
    )
    _validate_snapshot(
        json_snapshot,
        uri=H6_RELEASE_DATES_URI,
        source_format=OfficialSourceFormat.JSON,
    )
    visible = _H6HtmlParser()
    visible.feed(html_snapshot.content.decode("utf-8-sig"))
    visible.close()
    if "Money Stock Measures - H.6 Release" not in visible.visible_text or (
        "fourth Tuesday" not in visible.visible_text
    ):
        raise ValueError("H.6 landing page semantics differ")
    try:
        payload = json.loads(json_snapshot.content)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("H.6 release-date ledger is invalid JSON") from exc
    years = _sequence(payload, "H.6 release-date ledger")
    entries: list[FederalReserveH6IndexEntryV1] = []
    for year_item in years:
        year = _mapping(year_item, "H.6 year")
        for month_item in _sequence(year.get("Months"), "H.6 months"):
            month = _mapping(month_item, "H.6 month")
            for raw_item in _sequence(month.get("Dates"), "H.6 dates"):
                raw = _required_text(raw_item, "indexed date")
                if re.fullmatch(r"\d{8}(?:\*\*|\*\*\*)?", raw) is None:
                    raise ValueError("H.6 indexed date token differs")
                indexed_raw = raw[:8]
                release_raw = _INDEX_DATE_CORRECTIONS.get(
                    indexed_raw, indexed_raw
                )
                artifact_raw = _INDEX_ARTIFACT_DATES.get(
                    indexed_raw, indexed_raw
                )
                indexed = date(
                    int(indexed_raw[:4]),
                    int(indexed_raw[4:6]),
                    int(indexed_raw[6:]),
                ).isoformat()
                release = date(
                    int(release_raw[:4]),
                    int(release_raw[4:6]),
                    int(release_raw[6:]),
                ).isoformat()
                artifact_date = date(
                    int(artifact_raw[:4]),
                    int(artifact_raw[4:6]),
                    int(artifact_raw[6:]),
                ).isoformat()
                if not H6_PREDECESSOR_RELEASE_DATE <= release <= as_of:
                    continue
                limitations = [
                    "Seasonally adjusted M2 is reconstructed from the exact release table."
                ]
                if indexed != release:
                    limitations.append(_INDEX_CORRECTION_NOTES[indexed_raw])
                release_format = (
                    OfficialSourceFormat.TEXT
                    if release in _TEXT_RELEASE_DATES
                    else OfficialSourceFormat.HTML
                )
                if release_format is OfficialSourceFormat.TEXT:
                    limitations.append(
                        "The directory's default and screen-reader HTML are "
                        "stale May 16 copies; the official text and PDF both "
                        "author June 13 and contain the June 13 tables."
                    )
                if release in _TIMING_PDF_EXCLUDED_RELEASE_DATES:
                    limitations.append(
                        "The directory's h6.pdf is excluded from timing "
                        "evidence because it is a special notice without the "
                        "occurrence clock or a stale release."
                    )
                entries.append(
                    FederalReserveH6IndexEntryV1(
                        indexed_date=indexed,
                        release_date=release,
                        frequency=_frequency(release),
                        release_uri=(
                            _text_uri(artifact_date)
                            if release_format is OfficialSourceFormat.TEXT
                            else _release_uri(artifact_date)
                        ),
                        release_format=release_format,
                        timing_pdf_uri=(
                            _pdf_uri(artifact_date)
                            if _has_pdf(release)
                            else None
                        ),
                        annual_seasonal_review=(
                            raw.endswith("**") and not raw.endswith("***")
                        ),
                        first_monthly_release=raw.endswith("***"),
                        limitations=tuple(limitations),
                    )
                )
    return FederalReserveH6ReleaseIndexV1(
        as_of_date=as_of,
        index_page=_artifact(html_snapshot, "release-index-html"),
        release_dates=_artifact(json_snapshot, "release-dates-json"),
        entries=tuple(entries),
    )


def build_federal_reserve_h6_release_requests(
    registry: OfficialSourceRegistryV1,
    release_index: FederalReserveH6ReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan every exact release page and available timing PDF."""
    if not isinstance(release_index, FederalReserveH6ReleaseIndexV1):
        raise TypeError("H.6 release requests require a v1 index")
    requests: list[OfficialSourceRequestV1] = []
    for entry in release_index.entries:
        requests.append(
            _request(
                registry,
                entry.release_uri,
                entry.release_format,
                as_of_date=release_index.as_of_date,
            )
        )
        if entry.timing_pdf_uri is not None:
            requests.append(
                _request(
                    registry,
                    entry.timing_pdf_uri,
                    OfficialSourceFormat.PDF,
                    as_of_date=release_index.as_of_date,
                )
            )
    return tuple(requests)


def build_federal_reserve_h6_archive_manifest(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    release_index: FederalReserveH6ReleaseIndexV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
) -> FederalReserveH6ArchiveManifestV1:
    """Build all point-in-time H.6 M2 triplets from retained evidence."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("H.6 manifest requires a v1 registry")
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("H.6 manifest requires a v1 U.S. profile")
    if not isinstance(release_index, FederalReserveH6ReleaseIndexV1):
        raise TypeError("H.6 manifest requires a v1 release index")
    if profile.registry_id != registry.registry_id:
        raise ValueError("H.6 registry and profile identities differ")
    program = profile.by_key.get(H6_PROGRAM_KEY)
    if program is None or program.source_key != H6_SOURCE_KEY:
        raise ValueError("H.6 program source differs")
    required_uris = {
        H6_INDEX_URI,
        H6_RELEASE_DATES_URI,
        *(item.release_uri for item in release_index.entries),
        *(
            item.timing_pdf_uri
            for item in release_index.entries
            if item.timing_pdf_uri is not None
        ),
    }
    if set(snapshots_by_uri) != required_uris:
        raise ValueError("H.6 retained snapshot inventory differs")
    parsed: dict[str, _ParsedH6Release] = {}
    release_artifacts: dict[str, FederalReserveH6ArtifactV1] = {}
    pdf_artifacts: dict[str, FederalReserveH6ArtifactV1] = {}
    for entry in release_index.entries:
        release_snapshot = snapshots_by_uri[entry.release_uri]
        _validate_snapshot(
            release_snapshot,
            uri=entry.release_uri,
            source_format=entry.release_format,
        )
        parsed[entry.release_date] = _parse_release(
            release_snapshot.content, entry
        )
        release_artifacts[entry.release_date] = _artifact(
            release_snapshot,
            (
                "predecessor-artifact"
                if entry.release_date == H6_PREDECESSOR_RELEASE_DATE
                else "release-artifact"
            ),
        )
        if entry.timing_pdf_uri is not None:
            pdf_snapshot = snapshots_by_uri[entry.timing_pdf_uri]
            _validate_snapshot(
                pdf_snapshot,
                uri=entry.timing_pdf_uri,
                source_format=OfficialSourceFormat.PDF,
            )
            _validate_pdf_time(pdf_snapshot.content, entry.release_date)
            pdf_artifacts[entry.release_date] = _artifact(
                pdf_snapshot, "timing-pdf"
            )

    publications: list[FederalReserveH6PublicationV1] = []
    for offset, entry in enumerate(release_index.entries[1:], start=1):
        current = parsed[entry.release_date]
        predecessor_entry = release_index.entries[offset - 1]
        predecessor = parsed[predecessor_entry.release_date]
        rows = current.target_rows
        actual_reference, actual_lexical = rows[-1]
        previous_reference, revised_lexical = rows[-2]
        predecessor_rows = (
            predecessor.target_rows
            if entry.frequency is H6PublicationFrequency.WEEKLY
            else predecessor.monthly_rows
        )
        predecessor_values = dict(predecessor_rows)
        previous_lexical = predecessor_values.get(previous_reference)
        if previous_lexical is None:
            raise ValueError(
                "H.6 predecessor lacks the previous reference period"
            )
        actual_lexical, actual_value = _lexical_number(
            actual_lexical, "actual M2"
        )
        previous_lexical, previous_value = _lexical_number(
            previous_lexical, "previous-as-known M2"
        )
        revised_lexical, revised_value = _lexical_number(
            revised_lexical, "revised previous M2"
        )
        timing_pdf = pdf_artifacts.get(entry.release_date)
        exact = timing_pdf is not None or current.exact_time_from_release
        local_time = (
            "16:30"
            if entry.frequency is H6PublicationFrequency.WEEKLY
            else "13:00"
        )
        precision = (
            EconomicTimePrecision.EXACT_MINUTE
            if exact
            else EconomicTimePrecision.SCHEDULED_ONLY
        )
        released_at_ns = (
            normalize_official_source_timestamp(
                f"{entry.release_date}T{local_time}:00",
                H6_SOURCE_TIMEZONE,
                EconomicTimePrecision.EXACT_MINUTE,
            ).utc_ns
            if exact
            else None
        )
        publications.append(
            FederalReserveH6PublicationV1(
                release_date=entry.release_date,
                frequency=entry.frequency,
                reference_period=actual_reference,
                previous_reference_period=previous_reference,
                release_time_local=local_time,
                time_precision=precision,
                released_at_ns=released_at_ns,
                actual_lexical=actual_lexical,
                actual_value=actual_value,
                previous_as_known_lexical=previous_lexical,
                previous_as_known_value=previous_value,
                revised_previous_lexical=revised_lexical,
                revised_previous_value=revised_value,
                annual_seasonal_review=entry.annual_seasonal_review,
                first_monthly_release=entry.first_monthly_release,
                parser_era=current.parser_era,
                release_artifact=release_artifacts[entry.release_date],
                timing_pdf=timing_pdf,
                release_time_locator=(
                    "timing PDF first-page release header"
                    if timing_pdf is not None
                    else (
                        "release artifact occurrence-specific release note"
                        if current.exact_time_from_release
                        else "official H.6 release-dates schedule convention"
                    )
                ),
                value_locator=(
                    "Table 2 seasonally adjusted M2 week average"
                    if entry.frequency is H6PublicationFrequency.WEEKLY
                    else "Table 1 seasonally adjusted monthly M2"
                ),
                revision_locator=(
                    "current release previous-reference row compared with the preceding release initial vintage"
                ),
            )
        )

    all_artifacts = (
        release_index.index_page,
        release_index.release_dates,
        *release_artifacts.values(),
        *pdf_artifacts.values(),
    )
    return FederalReserveH6ArchiveManifestV1(
        registry_id=registry.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        predecessor_artifact=release_artifacts[H6_PREDECESSOR_RELEASE_DATE],
        publications=tuple(publications),
        raw_artifact_count=len(all_artifacts),
        total_content_bytes=sum(item.content_length for item in all_artifacts),
        release_artifact_bytes=sum(
            item.content_length for item in release_artifacts.values()
        ),
        timing_pdf_bytes=sum(
            item.content_length for item in pdf_artifacts.values()
        ),
        weekly_count=sum(
            item.frequency is H6PublicationFrequency.WEEKLY
            for item in publications
        ),
        monthly_count=sum(
            item.frequency is H6PublicationFrequency.MONTHLY
            for item in publications
        ),
        exact_minute_count=sum(
            item.time_precision is EconomicTimePrecision.EXACT_MINUTE
            for item in publications
        ),
        scheduled_only_count=sum(
            item.time_precision is EconomicTimePrecision.SCHEDULED_ONLY
            for item in publications
        ),
        revision_occurrence_count=sum(
            item.revision_delta != 0 for item in publications
        ),
    )


def replay_federal_reserve_h6_archive(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    expected: FederalReserveH6ArchiveManifestV1,
) -> FederalReserveH6ArchiveManifestV1:
    """Rebuild and compare the complete retained H.6 corpus."""
    rebuilt_index = parse_federal_reserve_h6_release_index(
        (
            snapshots_by_uri[H6_INDEX_URI],
            snapshots_by_uri[H6_RELEASE_DATES_URI],
        ),
        as_of_date=expected.release_index.as_of_date,
    )
    rebuilt = build_federal_reserve_h6_archive_manifest(
        registry, profile, rebuilt_index, snapshots_by_uri
    )
    if rebuilt != expected:
        raise ValueError("H.6 retained-corpus replay differs")
    return rebuilt


def federal_reserve_h6_coverage_from_manifest(
    profile: UnitedStatesBackfillProfileV1,
    manifest: FederalReserveH6ArchiveManifestV1,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete H.6 occurrence coverage from verified evidence."""
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("H.6 coverage requires a v1 U.S. profile")
    if not isinstance(manifest, FederalReserveH6ArchiveManifestV1):
        raise TypeError("H.6 coverage requires a v1 manifest")
    if (
        manifest.profile_id != profile.profile_id
        or manifest.registry_id != profile.registry_id
    ):
        raise ValueError("H.6 manifest differs from U.S. profile")
    program = profile.by_key[H6_PROGRAM_KEY]
    count = len(manifest.publications)
    coverage = UnitedStatesProgramCoverageV1(
        program_key=H6_PROGRAM_KEY,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=manifest.release_index.as_of_date,
        expected_occurrence_count=count,
        schedule_count=count,
        initial_actual_count=count,
        previous_as_known_count=count,
        revision_count=manifest.revision_occurrence_count,
        exact_minute_count=manifest.exact_minute_count,
        forecast_count=0,
        artifact_sha256s=tuple(
            item.release_artifact.content_sha256
            for item in manifest.publications
        ),
        gap_reasons=(UnitedStatesCoverageGapReason.NO_EVENT_FORECAST,),
        notes=(
            "Weekly M2 week-average releases and monthly M2 releases remain distinct native-frequency eras.",
            "Each previous-as-known value comes from the preceding release before comparison with the current revised row.",
            "The February 2021 frequency and M1-definition redesign remains explicit; M2 retains its comparable definition.",
            "Exact clocks require an occurrence PDF or authored release note; remaining occurrences retain the official scheduled-only convention.",
        ),
    )
    if not coverage.is_complete_for(program):
        raise ValueError("H.6 manifest does not qualify complete coverage")
    return coverage


def packaged_federal_reserve_h6_manifest_path() -> Path:
    return Path(__file__).with_name("assets") / "us_h6_archive_v1.json"


def packaged_federal_reserve_h6_indexes_path() -> Path:
    return Path(__file__).with_name("assets") / "us_h6_index_pages_v1.json"


def load_packaged_federal_reserve_h6_archive_manifest() -> (
    FederalReserveH6ArchiveManifestV1
):
    """Load and validate the compact packaged H.6 manifest."""
    return FederalReserveH6ArchiveManifestV1.from_json(
        packaged_federal_reserve_h6_manifest_path().read_text(encoding="utf-8")
    )


def load_packaged_federal_reserve_h6_index_pages() -> Mapping[str, bytes]:
    """Load and validate the exact packaged H.6 index receipts."""
    try:
        payload = json.loads(
            packaged_federal_reserve_h6_indexes_path().read_text(
                encoding="utf-8"
            )
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("packaged H.6 index pages are invalid") from exc
    root = _mapping(payload, "H.6 index pages")
    if root.get("schema_version") != H6_INDEX_PAGES_SCHEMA_VERSION:
        raise ValueError("packaged H.6 index-page schema differs")
    pages = _sequence(root.get("pages"), "H.6 index pages")
    result: dict[str, bytes] = {}
    for item in pages:
        row = _mapping(item, "H.6 index page")
        uri = _required_text(row.get("uri"), "uri")
        encoded_value = row.get("content_base64")
        if (
            not isinstance(encoded_value, str)
            or not encoded_value
            or len(encoded_value) > MAX_H6_ARTIFACT_BYTES * 2
        ):
            raise ValueError("packaged H.6 index page is invalid")
        encoded = encoded_value
        try:
            content = base64.b64decode(encoded, validate=True)
        except ValueError as exc:
            raise ValueError("packaged H.6 index page is not base64") from exc
        if not content or uri in result:
            raise ValueError("packaged H.6 index-page inventory differs")
        result[uri] = content
    if set(result) != {H6_INDEX_URI, H6_RELEASE_DATES_URI}:
        raise ValueError("packaged H.6 index-page URIs differ")
    return MappingProxyType(result)


__all__ = [
    "H6_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "H6_ARTIFACT_SCHEMA_VERSION",
    "H6_FINAL_WEEKLY_RELEASE_DATE",
    "H6_FIRST_MONTHLY_RELEASE_DATE",
    "H6_INDEX_ENTRY_SCHEMA_VERSION",
    "H6_INDEX_PAGES_SCHEMA_VERSION",
    "H6_INDEX_URI",
    "H6_LATEST_PACKAGED_RELEASE_DATE",
    "H6_PARSER_ID",
    "H6_PROGRAM_KEY",
    "H6_PUBLICATION_SCHEMA_VERSION",
    "H6_RELEASE_DATES_URI",
    "H6_RELEASE_INDEX_SCHEMA_VERSION",
    "H6_SOURCE_KEY",
    "FederalReserveH6ArchiveManifestV1",
    "FederalReserveH6ArtifactV1",
    "FederalReserveH6IndexEntryV1",
    "FederalReserveH6PublicationV1",
    "FederalReserveH6ReleaseIndexV1",
    "H6PublicationFrequency",
    "build_federal_reserve_h6_archive_manifest",
    "build_federal_reserve_h6_index_requests",
    "build_federal_reserve_h6_release_requests",
    "federal_reserve_h6_coverage_from_manifest",
    "load_packaged_federal_reserve_h6_archive_manifest",
    "load_packaged_federal_reserve_h6_index_pages",
    "packaged_federal_reserve_h6_indexes_path",
    "packaged_federal_reserve_h6_manifest_path",
    "parse_federal_reserve_h6_release_index",
    "replay_federal_reserve_h6_archive",
]
