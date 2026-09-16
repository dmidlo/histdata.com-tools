"""Philadelphia Fed MBOS occurrence-specific archive qualification.

The Manufacturing Business Outlook Survey (MBOS) publishes one current
general-activity diffusion index and one native six-month-future diffusion
index each month.  This module keeps those concepts distinct, preserves the
point-in-time previous values and seasonal revisions, and never substitutes
the current downloadable revised history for contemporaneous releases.

The official 1968--2007 ZIP supplies the December 1999 predecessor and every
2000--2007 report.  The live archive component enumerates each 2008-present
PDF and, for the current 2026 page era, its companion HTML report.  The
compact packaged assets contain hashes, normalized values, the exact archive
index receipt, and reviewed OCR for the image-only 1999--2001 reports; callers
may retain and replay the complete raw corpus.
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
from io import BytesIO
from itertools import product
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urljoin, urlsplit, urlunsplit
from zipfile import BadZipFile, ZipFile

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import (
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
from histdatacom.market_context.us_backfill import (
    US_BACKFILL_START_DATE,
    UnitedStatesBackfillProfileV1,
    UnitedStatesProgramCoverageV1,
)
from histdatacom.runtime_contracts import JSONValue

MBOS_ARTIFACT_SCHEMA_VERSION = "histdatacom.mbos-artifact.v1"
MBOS_INDEX_ENTRY_SCHEMA_VERSION = "histdatacom.mbos-index-entry.v1"
MBOS_RELEASE_INDEX_SCHEMA_VERSION = "histdatacom.mbos-release-index.v1"
MBOS_OCR_ENTRY_SCHEMA_VERSION = "histdatacom.mbos-ocr-entry.v1"
MBOS_OCR_CORPUS_SCHEMA_VERSION = "histdatacom.mbos-ocr-corpus.v1"
MBOS_PUBLICATION_SCHEMA_VERSION = "histdatacom.mbos-publication.v1"
MBOS_ARCHIVE_MANIFEST_SCHEMA_VERSION = "histdatacom.mbos-archive-manifest.v1"

MBOS_SOURCE_KEY = "us.frb.philadelphia-surveys"
MBOS_PROGRAM_KEY = "us.philadelphia-fed.mbos"
MBOS_INDEX_URI = (
    "https://www.philadelphiafed.org/surveys-and-data/"
    "regional-economic-analysis/manufacturing-business-outlook-survey"
)
MBOS_ARCHIVE_URI = (
    "https://www.philadelphiafed.org/surveys-and-data/mbos-archives"
)
MBOS_BULK_ARCHIVE_URI = (
    "https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/"
    "MBOS/Archives/MBOS-1968_2007.zip"
)
MBOS_FIRST_PERIOD = "2000-01"
MBOS_PREDECESSOR_PERIOD = "1999-12"
MBOS_LATEST_PACKAGED_PERIOD = "2026-08"
MBOS_SOURCE_TIMEZONE = "America/New_York"

MAX_MBOS_RELEASES = 512
MAX_MBOS_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_MBOS_TOTAL_BYTES = 256 * 1024 * 1024
MAX_MBOS_OCR_TEXT_BYTES = 2 * 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PERIOD_RE = re.compile(r"^(?P<year>\d{4})-(?P<month>0[1-9]|1[0-2])$")
_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_NUMBER_RE = re.compile(r"(?<![A-Za-z0-9])[-+]?\d+(?:\.\d+)?")
_MONTH_NUMBER = {
    name.lower(): number
    for number, name in enumerate(calendar.month_name)
    if name
}
_MONTH_NUMBER.update(
    {
        name.lower(): number
        for number, name in enumerate(calendar.month_abbr)
        if name
    }
)
_MONTH_NUMBER["sept"] = 9

# Hash-bound corrections for damaged OCR text objects and one duplicated PDF
# text layer.  Values were independently checked against the source narrative
# and the diffusion identity (increase share minus decrease share).
_VALUE_OVERRIDES = MappingProxyType(
    {
        "726e94080bf17130cf820cf9ed955fb78580e4ab15aa655473f77b99b2f17cac": (
            "25.0",
            "11.7",
        ),
        "6618d4da79488c58e4f2834f5adbd52b20473d859e371657afc5c4cbd5b8a9cc": (
            "1.7",
            "12.0",
        ),
        "f2fd3908cd64e58aaeab724d9912cb8e50c31239a379343940383c255b3b58ac": (
            "-30.5",
            "4.8",
        ),
        "2b72732cb0953e9ca0d059c6299285c53fd84181675926d727743912b3152199": (
            "-23.5",
            "20.9",
        ),
        "58dd69fa4d2f339c6fd7fd7ee9258e8a05eddc13a2173a9d1abdb68654e3ed29": (
            "-27.4",
            "52.3",
        ),
        "61fa0065b37a42031cce87778aca3b54f21e22a867dc7de973ea1d9de06ee4e9": (
            "-5.5",
            "45.5",
        ),
        "033f7d3a12bb81d1094527b0b49b48146d97c958c0687a060abadc4fb1faaa8b": (
            "25.3",
            "27.5",
        ),
    }
)
_JANUARY_REVISED_PREVIOUS = MappingProxyType(
    {
        "2000-01": ("15.1", "26.7"),
        "2001-01": ("-4.2", "-4.2"),
        "2002-01": ("-12.6", "46.7"),
        "2003-01": ("11.3", "52.2"),
    }
)

# The April 2005 PDF contains overlapping duplicate text objects in the table
# row.  Bind the repaired row to the exact official content hash rather than
# accepting an arbitrary substring from that malformed text layer.
_TOKEN_OVERRIDES = MappingProxyType(
    {
        "033f7d3a12bb81d1094527b0b49b48146d97c958c0687a060abadc4fb1faaa8b": (
            "11.4",
            "35.8",
            "53.7",
            "10.5",
            "25.3",
            "29.8",
            "40.8",
            "43.7",
            "13.3",
            "27.5",
        )
    }
)


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


def _sha256(value: object, name: str) -> str:
    text = _required_text(value, name)
    if _SHA256_RE.fullmatch(text) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return text


def _iso_date(value: object, name: str) -> str:
    text = _required_text(value, name)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != text:
        raise ValueError(f"{name} must be a canonical ISO date")
    return text


def _period(value: object, name: str = "period") -> str:
    text = _required_text(value, name)
    if _PERIOD_RE.fullmatch(text) is None:
        raise ValueError(f"{name} must be a canonical year-month")
    return text


def _next_period(value: str) -> str:
    match = _PERIOD_RE.fullmatch(value)
    if match is None:
        raise ValueError("MBOS period is invalid")
    year = int(match.group("year"))
    month = int(match.group("month"))
    if month == 12:
        return f"{year + 1:04d}-01"
    return f"{year:04d}-{month + 1:02d}"


def _periods(first: str, last: str) -> tuple[str, ...]:
    result: list[str] = []
    current = first
    while current <= last:
        result.append(current)
        current = _next_period(current)
    return tuple(result)


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
    text = _required_text(value, name).replace("+", "")
    if re.fullmatch(r"-?\d+\.\d", text) is None:
        raise ValueError(f"{name} must retain one decimal place")
    number = _finite(text, name)
    if f"{number:.1f}" != text and not (number == 0 and text == "-0.0"):
        raise ValueError(f"{name} is not canonical")
    return text, number


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _normalize_uri(uri: str) -> str:
    parts = urlsplit(_required_text(uri, "uri"))
    if parts.scheme != "https" or not parts.netloc:
        raise ValueError("MBOS URI must use HTTPS")
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _third_thursday(period: str) -> str:
    match = _PERIOD_RE.fullmatch(period)
    if match is None:
        raise ValueError("MBOS period is invalid")
    year = int(match.group("year"))
    month = int(match.group("month"))
    first = date(year, month, 1)
    offset = (3 - first.weekday()) % 7
    return date(year, month, 1 + offset + 14).isoformat()


@dataclass(frozen=True, slots=True)
class PhiladelphiaFedMbosArtifactV1:
    """One raw or archive-member artifact bound by content hash."""

    role: str
    source_uri: str
    source_format: str
    content_sha256: str
    content_length: int
    archive_member: str | None = None
    container_sha256: str | None = None
    artifact_id: str = ""
    schema_version: str = MBOS_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MBOS_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported MBOS artifact schema")
        role = _required_text(self.role, "role")
        uri = _normalize_uri(self.source_uri)
        source_format = _required_text(self.source_format, "source_format")
        if source_format not in {"html", "pdf", "archive"}:
            raise ValueError("unsupported MBOS artifact format")
        digest = _sha256(self.content_sha256, "content_sha256")
        if (
            not isinstance(self.content_length, int)
            or isinstance(self.content_length, bool)
            or not 1 <= self.content_length <= MAX_MBOS_ARTIFACT_BYTES
        ):
            raise ValueError("MBOS artifact length is outside its bound")
        member = _optional_text(self.archive_member, "archive_member")
        container = (
            None
            if self.container_sha256 is None
            else _sha256(self.container_sha256, "container_sha256")
        )
        if (member is None) != (container is None):
            raise ValueError("MBOS member and container evidence must pair")
        if member is not None and (
            source_format != "pdf" or ".." in member or member.startswith("/")
        ):
            raise ValueError("MBOS archive member is invalid")
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "source_uri", uri)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "content_sha256", digest)
        object.__setattr__(self, "archive_member", member)
        object.__setattr__(self, "container_sha256", container)
        expected = _stable_id("mbos-artifact", self.identity_payload())
        if self.artifact_id and self.artifact_id != expected:
            raise ValueError("MBOS artifact identity differs")
        object.__setattr__(self, "artifact_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role,
            "source_uri": self.source_uri,
            "source_format": self.source_format,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "archive_member": self.archive_member,
            "container_sha256": self.container_sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "artifact_id": self.artifact_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PhiladelphiaFedMbosArtifactV1:
        return cls(
            role=str(data.get("role", "")),
            source_uri=str(data.get("source_uri", "")),
            source_format=str(data.get("source_format", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            archive_member=(
                None
                if data.get("archive_member") is None
                else str(data.get("archive_member"))
            ),
            container_sha256=(
                None
                if data.get("container_sha256") is None
                else str(data.get("container_sha256"))
            ),
            artifact_id=str(data.get("artifact_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PhiladelphiaFedMbosIndexEntryV1:
    """One monthly PDF discovered in the official MBOS archive."""

    reference_period: str
    title: str
    artifact_uri: str
    archive_member: str | None
    release_page_uri: str | None
    entry_id: str = ""
    schema_version: str = MBOS_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MBOS_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported MBOS index-entry schema")
        period = _period(self.reference_period, "reference_period")
        title = _required_text(self.title, "title")
        artifact_uri = _normalize_uri(self.artifact_uri)
        member = _optional_text(self.archive_member, "archive_member")
        page = (
            None
            if self.release_page_uri is None
            else _normalize_uri(self.release_page_uri)
        )
        if period <= "2007-12":
            if artifact_uri != MBOS_BULK_ARCHIVE_URI or member is None:
                raise ValueError("legacy MBOS entry must name its ZIP member")
        elif member is not None or not artifact_uri.lower().endswith(".pdf"):
            raise ValueError("modern MBOS entry must name one direct PDF")
        if page is not None and not page.endswith(period):
            raise ValueError("MBOS release-page period differs")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "artifact_uri", artifact_uri)
        object.__setattr__(self, "archive_member", member)
        object.__setattr__(self, "release_page_uri", page)
        expected = _stable_id("mbos-index-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("MBOS index-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    @property
    def source_locator(self) -> str:
        if self.archive_member is None:
            return self.artifact_uri
        return f"{self.artifact_uri}#{self.archive_member}"

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "title": self.title,
            "artifact_uri": self.artifact_uri,
            "archive_member": self.archive_member,
            "release_page_uri": self.release_page_uri,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PhiladelphiaFedMbosIndexEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            title=str(data.get("title", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            archive_member=(
                None
                if data.get("archive_member") is None
                else str(data.get("archive_member"))
            ),
            release_page_uri=(
                None
                if data.get("release_page_uri") is None
                else str(data.get("release_page_uri"))
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PhiladelphiaFedMbosReleaseIndexV1:
    """Complete monthly archive inventory including one predecessor."""

    as_of_date: str
    archive_index: PhiladelphiaFedMbosArtifactV1
    bulk_archive: PhiladelphiaFedMbosArtifactV1
    entries: tuple[PhiladelphiaFedMbosIndexEntryV1, ...]
    index_id: str = ""
    schema_version: str = MBOS_RELEASE_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MBOS_RELEASE_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported MBOS release-index schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        if self.archive_index.role != "archive-index":
            raise ValueError("MBOS archive-index artifact differs")
        if self.bulk_archive.role != "bulk-archive":
            raise ValueError("MBOS bulk-archive artifact differs")
        entries = tuple(self.entries)
        if not 2 <= len(entries) <= MAX_MBOS_RELEASES:
            raise ValueError("MBOS release inventory is outside its bound")
        periods = tuple(item.reference_period for item in entries)
        expected_periods = _periods(MBOS_PREDECESSOR_PERIOD, periods[-1])
        if periods != expected_periods:
            raise ValueError("MBOS release inventory is not continuous")
        if periods[-1] > as_of[:7]:
            raise ValueError("MBOS release inventory exceeds as-of date")
        if len({item.source_locator for item in entries}) != len(entries):
            raise ValueError("MBOS release inventory repeats an artifact")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id("mbos-release-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("MBOS release-index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_period(self) -> Mapping[str, PhiladelphiaFedMbosIndexEntryV1]:
        return MappingProxyType(
            {item.reference_period: item for item in self.entries}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "archive_index": self.archive_index.to_dict(),
            "bulk_archive": self.bulk_archive.to_dict(),
            "entries": [item.to_dict() for item in self.entries],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PhiladelphiaFedMbosReleaseIndexV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            archive_index=PhiladelphiaFedMbosArtifactV1.from_dict(
                _mapping(data.get("archive_index"), "archive_index")
            ),
            bulk_archive=PhiladelphiaFedMbosArtifactV1.from_dict(
                _mapping(data.get("bulk_archive"), "bulk_archive")
            ),
            entries=tuple(
                PhiladelphiaFedMbosIndexEntryV1.from_dict(
                    _mapping(item, "MBOS index entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PhiladelphiaFedMbosOcrEntryV1:
    """Reviewed two-page OCR bound to one image-only official report."""

    source_locator: str
    content_sha256: str
    page_texts: tuple[str, str]
    entry_id: str = ""
    schema_version: str = MBOS_OCR_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MBOS_OCR_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported MBOS OCR-entry schema")
        locator = _required_text(self.source_locator, "source_locator")
        if not locator.startswith("https://"):
            raise ValueError("MBOS OCR locator must use HTTPS")
        digest = _sha256(self.content_sha256, "content_sha256")
        pages = tuple(self.page_texts)
        if len(pages) != 2 or any(not item.strip() for item in pages):
            raise ValueError("MBOS OCR requires two non-empty pages")
        if sum(len(item.encode("utf-8")) for item in pages) > (
            MAX_MBOS_OCR_TEXT_BYTES
        ):
            raise ValueError("MBOS OCR text exceeds its bound")
        object.__setattr__(self, "source_locator", locator)
        object.__setattr__(self, "content_sha256", digest)
        object.__setattr__(self, "page_texts", pages)
        expected = _stable_id("mbos-ocr-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("MBOS OCR-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_locator": self.source_locator,
            "content_sha256": self.content_sha256,
            "page_texts": list(self.page_texts),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PhiladelphiaFedMbosOcrEntryV1:
        pages = tuple(
            str(item)
            for item in _sequence(data.get("page_texts"), "page_texts")
        )
        return cls(
            source_locator=str(data.get("source_locator", "")),
            content_sha256=str(data.get("content_sha256", "")),
            page_texts=cast(tuple[str, str], pages),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PhiladelphiaFedMbosOcrCorpusV1:
    """Deterministic reviewed OCR configuration and evidence corpus."""

    engine: str
    engine_version: str
    render_dpi: int
    page_modes: tuple[int, int]
    entries: tuple[PhiladelphiaFedMbosOcrEntryV1, ...]
    corpus_id: str = ""
    schema_version: str = MBOS_OCR_CORPUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MBOS_OCR_CORPUS_SCHEMA_VERSION:
            raise ValueError("unsupported MBOS OCR-corpus schema")
        if _required_text(self.engine, "engine") != "tesseract":
            raise ValueError("MBOS OCR engine differs")
        version = _required_text(self.engine_version, "engine_version")
        if self.render_dpi != 300 or self.page_modes != (6, 6):
            raise ValueError("MBOS OCR settings differ")
        entries = tuple(self.entries)
        if len(entries) != 25:
            raise ValueError("MBOS OCR inventory differs")
        locators = tuple(item.source_locator for item in entries)
        if locators != tuple(sorted(locators)) or len(set(locators)) != 25:
            raise ValueError("MBOS OCR entries must be unique and sorted")
        object.__setattr__(self, "engine_version", version)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id("mbos-ocr-corpus", self.identity_payload())
        if self.corpus_id and self.corpus_id != expected:
            raise ValueError("MBOS OCR-corpus identity differs")
        object.__setattr__(self, "corpus_id", expected)

    @property
    def by_locator(self) -> Mapping[str, PhiladelphiaFedMbosOcrEntryV1]:
        return MappingProxyType(
            {item.source_locator: item for item in self.entries}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "engine": self.engine,
            "engine_version": self.engine_version,
            "render_dpi": self.render_dpi,
            "page_modes": list(self.page_modes),
            "entries": [item.to_dict() for item in self.entries],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "corpus_id": self.corpus_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PhiladelphiaFedMbosOcrCorpusV1:
        modes = tuple(
            cast(int, item)
            for item in _sequence(data.get("page_modes"), "page_modes")
        )
        return cls(
            engine=str(data.get("engine", "")),
            engine_version=str(data.get("engine_version", "")),
            render_dpi=cast(int, data.get("render_dpi")),
            page_modes=cast(tuple[int, int], modes),
            entries=tuple(
                PhiladelphiaFedMbosOcrEntryV1.from_dict(
                    _mapping(item, "MBOS OCR entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            corpus_id=str(data.get("corpus_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> PhiladelphiaFedMbosOcrCorpusV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("MBOS OCR corpus is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "MBOS OCR corpus"))


@dataclass(frozen=True, slots=True)
class PhiladelphiaFedMbosPublicationV1:
    """One point-in-time monthly MBOS current/future observation."""

    reference_period: str
    previous_reference_period: str
    release_date: str
    release_time_local: str | None
    time_precision: EconomicTimePrecision
    released_at_ns: int | None
    current_index_lexical: str
    current_index: float
    future_index_lexical: str
    future_index: float
    previous_current_lexical: str
    previous_current: float
    previous_future_lexical: str
    previous_future: float
    revised_previous_current_lexical: str
    revised_previous_current: float
    revised_previous_future_lexical: str
    revised_previous_future: float
    revision_notice: bool
    parser_era: str
    release_pdf: PhiladelphiaFedMbosArtifactV1
    release_page: PhiladelphiaFedMbosArtifactV1 | None
    ocr_entry_id: str | None
    release_locator: str
    value_locator: str
    revision_locator: str
    publication_id: str = ""
    schema_version: str = MBOS_PUBLICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MBOS_PUBLICATION_SCHEMA_VERSION:
            raise ValueError("unsupported MBOS publication schema")
        period = _period(self.reference_period, "reference_period")
        previous = _period(
            self.previous_reference_period, "previous_reference_period"
        )
        if _next_period(previous) != period:
            raise ValueError("MBOS previous period is not consecutive")
        release_date = _iso_date(self.release_date, "release_date")
        local = _optional_text(self.release_time_local, "release_time_local")
        precision = EconomicTimePrecision.from_value(self.time_precision)
        if local is not None and _TIME_RE.fullmatch(local) is None:
            raise ValueError("MBOS release time must be HH:MM")
        if precision is EconomicTimePrecision.EXACT_MINUTE:
            if local is None or self.released_at_ns is None:
                raise ValueError("exact MBOS time requires an instant")
            expected_ns = normalize_official_source_timestamp(
                f"{release_date}T{local}:00",
                MBOS_SOURCE_TIMEZONE,
                EconomicTimePrecision.EXACT_MINUTE,
            ).utc_ns
            if self.released_at_ns != expected_ns:
                raise ValueError("MBOS release instant differs")
        elif precision is EconomicTimePrecision.INFERRED_BOUNDED:
            if self.released_at_ns is not None:
                raise ValueError("inferred MBOS time cannot invent an instant")
        else:
            raise ValueError("unsupported MBOS publication precision")
        pairs = (
            ("current_index", self.current_index_lexical, self.current_index),
            ("future_index", self.future_index_lexical, self.future_index),
            (
                "previous_current",
                self.previous_current_lexical,
                self.previous_current,
            ),
            (
                "previous_future",
                self.previous_future_lexical,
                self.previous_future,
            ),
            (
                "revised_previous_current",
                self.revised_previous_current_lexical,
                self.revised_previous_current,
            ),
            (
                "revised_previous_future",
                self.revised_previous_future_lexical,
                self.revised_previous_future,
            ),
        )
        for name, lexical, numeric in pairs:
            normalized, value = _lexical_number(lexical, name)
            if normalized != lexical or value != numeric:
                raise ValueError(
                    f"MBOS {name} numeric and lexical values differ"
                )
        if not isinstance(self.revision_notice, bool):
            raise TypeError("revision_notice must be boolean")
        era = _required_text(self.parser_era, "parser_era")
        if era not in {
            "ocr-pdf-eight-column",
            "native-pdf-eight-column",
            "native-pdf-ten-column",
        }:
            raise ValueError("unsupported MBOS parser era")
        if self.release_pdf.role != "release-pdf":
            raise ValueError("MBOS publication PDF artifact differs")
        page = self.release_page
        if page is not None and page.role != "release-page":
            raise ValueError("MBOS publication page artifact differs")
        ocr_id = _optional_text(self.ocr_entry_id, "ocr_entry_id")
        if (era == "ocr-pdf-eight-column") != (ocr_id is not None):
            raise ValueError("MBOS OCR identity differs from parser era")
        if ocr_id is not None and not ocr_id.startswith(
            "mbos-ocr-entry:sha256:"
        ):
            raise ValueError("MBOS OCR identity is invalid")
        if period >= "2026-01" and page is None:
            raise ValueError("2026 MBOS publication requires its HTML page")
        if period < "2026-01" and page is not None:
            raise ValueError("legacy MBOS publication has an unexpected page")
        for name in ("release_locator", "value_locator", "revision_locator"):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "previous_reference_period", previous)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "release_time_local", local)
        object.__setattr__(self, "time_precision", precision)
        object.__setattr__(self, "parser_era", era)
        object.__setattr__(self, "ocr_entry_id", ocr_id)
        expected = _stable_id("mbos-publication", self.identity_payload())
        if self.publication_id and self.publication_id != expected:
            raise ValueError("MBOS publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    @property
    def current_revision_delta(self) -> float:
        return round(self.revised_previous_current - self.previous_current, 1)

    @property
    def future_revision_delta(self) -> float:
        return round(self.revised_previous_future - self.previous_future, 1)

    @property
    def revision_record_count(self) -> int:
        return int(self.current_revision_delta != 0) + int(
            self.future_revision_delta != 0
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "previous_reference_period": self.previous_reference_period,
            "release_date": self.release_date,
            "release_time_local": self.release_time_local,
            "time_precision": self.time_precision.value,
            "released_at_ns": self.released_at_ns,
            "current_index_lexical": self.current_index_lexical,
            "current_index": self.current_index,
            "future_index_lexical": self.future_index_lexical,
            "future_index": self.future_index,
            "previous_current_lexical": self.previous_current_lexical,
            "previous_current": self.previous_current,
            "previous_future_lexical": self.previous_future_lexical,
            "previous_future": self.previous_future,
            "revised_previous_current_lexical": (
                self.revised_previous_current_lexical
            ),
            "revised_previous_current": self.revised_previous_current,
            "revised_previous_future_lexical": (
                self.revised_previous_future_lexical
            ),
            "revised_previous_future": self.revised_previous_future,
            "revision_notice": self.revision_notice,
            "parser_era": self.parser_era,
            "release_pdf": self.release_pdf.to_dict(),
            "release_page": (
                None
                if self.release_page is None
                else self.release_page.to_dict()
            ),
            "ocr_entry_id": self.ocr_entry_id,
            "release_locator": self.release_locator,
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
    ) -> PhiladelphiaFedMbosPublicationV1:
        page = data.get("release_page")
        return cls(
            reference_period=str(data.get("reference_period", "")),
            previous_reference_period=str(
                data.get("previous_reference_period", "")
            ),
            release_date=str(data.get("release_date", "")),
            release_time_local=(
                None
                if data.get("release_time_local") is None
                else str(data.get("release_time_local"))
            ),
            time_precision=EconomicTimePrecision.from_value(
                str(data.get("time_precision", ""))
            ),
            released_at_ns=cast(int | None, data.get("released_at_ns")),
            current_index_lexical=str(data.get("current_index_lexical", "")),
            current_index=cast(float, data.get("current_index")),
            future_index_lexical=str(data.get("future_index_lexical", "")),
            future_index=cast(float, data.get("future_index")),
            previous_current_lexical=str(
                data.get("previous_current_lexical", "")
            ),
            previous_current=cast(float, data.get("previous_current")),
            previous_future_lexical=str(
                data.get("previous_future_lexical", "")
            ),
            previous_future=cast(float, data.get("previous_future")),
            revised_previous_current_lexical=str(
                data.get("revised_previous_current_lexical", "")
            ),
            revised_previous_current=cast(
                float, data.get("revised_previous_current")
            ),
            revised_previous_future_lexical=str(
                data.get("revised_previous_future_lexical", "")
            ),
            revised_previous_future=cast(
                float, data.get("revised_previous_future")
            ),
            revision_notice=cast(bool, data.get("revision_notice")),
            parser_era=str(data.get("parser_era", "")),
            release_pdf=PhiladelphiaFedMbosArtifactV1.from_dict(
                _mapping(data.get("release_pdf"), "release_pdf")
            ),
            release_page=(
                None
                if page is None
                else PhiladelphiaFedMbosArtifactV1.from_dict(
                    _mapping(page, "release_page")
                )
            ),
            ocr_entry_id=(
                None
                if data.get("ocr_entry_id") is None
                else str(data.get("ocr_entry_id"))
            ),
            release_locator=str(data.get("release_locator", "")),
            value_locator=str(data.get("value_locator", "")),
            revision_locator=str(data.get("revision_locator", "")),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PhiladelphiaFedMbosArchiveManifestV1:
    """Complete content-addressed MBOS archive manifest."""

    registry_id: str
    profile_id: str
    release_index: PhiladelphiaFedMbosReleaseIndexV1
    ocr_corpus_id: str
    predecessor: PhiladelphiaFedMbosArtifactV1
    predecessor_current_lexical: str
    predecessor_current: float
    predecessor_future_lexical: str
    predecessor_future: float
    publications: tuple[PhiladelphiaFedMbosPublicationV1, ...]
    raw_artifact_count: int
    total_content_bytes: int
    publication_pdf_bytes: int
    exact_minute_count: int
    inferred_bounded_count: int
    forecast_occurrence_count: int
    revision_record_count: int
    revision_publication_count: int
    ocr_publication_count: int
    manifest_id: str = ""
    schema_version: str = MBOS_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MBOS_ARCHIVE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported MBOS archive-manifest schema")
        registry = _required_text(self.registry_id, "registry_id")
        profile = _required_text(self.profile_id, "profile_id")
        if not registry.startswith("official-source-registry:sha256:"):
            raise ValueError("MBOS registry identity is invalid")
        if not profile.startswith("us-backfill-profile:sha256:"):
            raise ValueError("MBOS profile identity is invalid")
        ocr_id = _required_text(self.ocr_corpus_id, "ocr_corpus_id")
        if not ocr_id.startswith("mbos-ocr-corpus:sha256:"):
            raise ValueError("MBOS OCR-corpus identity is invalid")
        if self.predecessor.role != "predecessor-pdf":
            raise ValueError("MBOS predecessor artifact differs")
        pred_current_lexical, pred_current = _lexical_number(
            self.predecessor_current_lexical, "predecessor_current"
        )
        pred_future_lexical, pred_future = _lexical_number(
            self.predecessor_future_lexical, "predecessor_future"
        )
        if (
            pred_current != self.predecessor_current
            or pred_future != self.predecessor_future
        ):
            raise ValueError("MBOS predecessor values differ")
        publications = tuple(self.publications)
        expected_periods = _periods(
            MBOS_FIRST_PERIOD,
            self.release_index.entries[-1].reference_period,
        )
        if tuple(item.reference_period for item in publications) != (
            expected_periods
        ):
            raise ValueError("MBOS publication inventory differs")
        if len(publications) + 1 != len(self.release_index.entries):
            raise ValueError("MBOS publication and index counts differ")
        prior_current = pred_current
        prior_future = pred_future
        for publication, entry in zip(
            publications, self.release_index.entries[1:], strict=True
        ):
            if publication.reference_period != entry.reference_period:
                raise ValueError("MBOS publication and index periods differ")
            if publication.release_pdf.source_uri != entry.artifact_uri:
                raise ValueError("MBOS publication and index artifacts differ")
            if publication.release_pdf.archive_member != entry.archive_member:
                raise ValueError("MBOS publication ZIP member differs")
            if (
                publication.previous_current != prior_current
                or publication.previous_future != prior_future
            ):
                raise ValueError("MBOS previous-as-known chain differs")
            prior_current = publication.current_index
            prior_future = publication.future_index
        release_hashes = {
            self.predecessor.content_sha256,
            *(item.release_pdf.content_sha256 for item in publications),
        }
        if len(release_hashes) != len(publications) + 1:
            raise ValueError("MBOS release hashes are not unique")
        expected_counts = (
            2
            + sum(
                item.release_pdf.archive_member is None for item in publications
            )
            + sum(item.release_page is not None for item in publications),
            sum(item.release_pdf.content_length for item in publications)
            + self.predecessor.content_length,
            sum(
                item.time_precision is EconomicTimePrecision.EXACT_MINUTE
                for item in publications
            ),
            sum(
                item.time_precision is EconomicTimePrecision.INFERRED_BOUNDED
                for item in publications
            ),
            len(publications),
            sum(item.revision_record_count for item in publications),
            sum(item.revision_record_count > 0 for item in publications),
            sum(item.ocr_entry_id is not None for item in publications),
        )
        actual_counts = (
            self.raw_artifact_count,
            self.publication_pdf_bytes,
            self.exact_minute_count,
            self.inferred_bounded_count,
            self.forecast_occurrence_count,
            self.revision_record_count,
            self.revision_publication_count,
            self.ocr_publication_count,
        )
        if expected_counts != actual_counts:
            raise ValueError("MBOS manifest summary counts differ")
        if (
            not isinstance(self.total_content_bytes, int)
            or not 1 <= self.total_content_bytes <= MAX_MBOS_TOTAL_BYTES
        ):
            raise ValueError("MBOS retained corpus exceeds its byte bound")
        if self.exact_minute_count + self.inferred_bounded_count != len(
            publications
        ):
            raise ValueError("MBOS time-precision counts differ")
        object.__setattr__(self, "registry_id", registry)
        object.__setattr__(self, "profile_id", profile)
        object.__setattr__(self, "ocr_corpus_id", ocr_id)
        object.__setattr__(
            self, "predecessor_current_lexical", pred_current_lexical
        )
        object.__setattr__(
            self, "predecessor_future_lexical", pred_future_lexical
        )
        object.__setattr__(self, "publications", publications)
        expected = _stable_id("mbos-archive-manifest", self.identity_payload())
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("MBOS archive-manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def by_period(self) -> Mapping[str, PhiladelphiaFedMbosPublicationV1]:
        return MappingProxyType(
            {item.reference_period: item for item in self.publications}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "ocr_corpus_id": self.ocr_corpus_id,
            "predecessor": self.predecessor.to_dict(),
            "predecessor_current_lexical": self.predecessor_current_lexical,
            "predecessor_current": self.predecessor_current,
            "predecessor_future_lexical": self.predecessor_future_lexical,
            "predecessor_future": self.predecessor_future,
            "publications": [item.to_dict() for item in self.publications],
            "raw_artifact_count": self.raw_artifact_count,
            "total_content_bytes": self.total_content_bytes,
            "publication_pdf_bytes": self.publication_pdf_bytes,
            "exact_minute_count": self.exact_minute_count,
            "inferred_bounded_count": self.inferred_bounded_count,
            "forecast_occurrence_count": self.forecast_occurrence_count,
            "revision_record_count": self.revision_record_count,
            "revision_publication_count": self.revision_publication_count,
            "ocr_publication_count": self.ocr_publication_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PhiladelphiaFedMbosArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=PhiladelphiaFedMbosReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "release_index")
            ),
            ocr_corpus_id=str(data.get("ocr_corpus_id", "")),
            predecessor=PhiladelphiaFedMbosArtifactV1.from_dict(
                _mapping(data.get("predecessor"), "predecessor")
            ),
            predecessor_current_lexical=str(
                data.get("predecessor_current_lexical", "")
            ),
            predecessor_current=cast(float, data.get("predecessor_current")),
            predecessor_future_lexical=str(
                data.get("predecessor_future_lexical", "")
            ),
            predecessor_future=cast(float, data.get("predecessor_future")),
            publications=tuple(
                PhiladelphiaFedMbosPublicationV1.from_dict(
                    _mapping(item, "MBOS publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            publication_pdf_bytes=cast(int, data.get("publication_pdf_bytes")),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            inferred_bounded_count=cast(
                int, data.get("inferred_bounded_count")
            ),
            forecast_occurrence_count=cast(
                int, data.get("forecast_occurrence_count")
            ),
            revision_record_count=cast(int, data.get("revision_record_count")),
            revision_publication_count=cast(
                int, data.get("revision_publication_count")
            ),
            ocr_publication_count=cast(int, data.get("ocr_publication_count")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> PhiladelphiaFedMbosArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("MBOS manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "MBOS manifest"))


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    as_of_date: str,
) -> OfficialSourceRequestV1:
    source = registry.source(MBOS_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=MBOS_PREDECESSOR_PERIOD + "-01",
        window_end=as_of_date,
    )


def build_philadelphia_fed_mbos_support_requests(
    registry: OfficialSourceRegistryV1, *, as_of_date: str
) -> tuple[OfficialSourceRequestV1, OfficialSourceRequestV1]:
    """Plan the official live index and 1968--2007 bulk archive."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("MBOS requests require a v1 registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    return (
        _request(
            registry,
            MBOS_INDEX_URI,
            OfficialSourceFormat.HTML,
            as_of_date=as_of,
        ),
        _request(
            registry,
            MBOS_BULK_ARCHIVE_URI,
            OfficialSourceFormat.ARCHIVE,
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
        raise TypeError("MBOS evidence requires a v1 snapshot")
    if (
        snapshot.request.source_key != MBOS_SOURCE_KEY
        or snapshot.request.uri != uri
        or snapshot.request.source_format is not source_format
        or snapshot.status_code != 200
        or not snapshot.content
        or len(snapshot.content) > MAX_MBOS_ARTIFACT_BYTES
    ):
        raise ValueError("MBOS official snapshot differs")


def _snapshot_artifact(
    snapshot: OfficialRawSnapshotV1,
    role: str,
    *,
    source_format: str | None = None,
) -> PhiladelphiaFedMbosArtifactV1:
    return PhiladelphiaFedMbosArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=(
            snapshot.request.source_format.value
            if source_format is None
            else source_format
        ),
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
    )


def _member_artifact(
    entry: PhiladelphiaFedMbosIndexEntryV1,
    content: bytes,
    *,
    container_sha256: str,
    role: str,
) -> PhiladelphiaFedMbosArtifactV1:
    return PhiladelphiaFedMbosArtifactV1(
        role=role,
        source_uri=entry.artifact_uri,
        source_format="pdf",
        content_sha256=hashlib.sha256(content).hexdigest(),
        content_length=len(content),
        archive_member=entry.archive_member,
        container_sha256=container_sha256,
    )


def _embedded_archive_payload(content: bytes) -> Mapping[str, Any]:
    try:
        source = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("MBOS archive page is not UTF-8") from exc
    marker = "name: 'data-periodic-release-archive-filter'"
    marker_at = source.find(marker)
    if marker_at < 0:
        raise ValueError("MBOS archive component is missing")
    data_at = source.find("data:", marker_at)
    if data_at < 0:
        raise ValueError("MBOS archive component data is missing")
    try:
        value, _ = json.JSONDecoder().raw_decode(source[data_at + 5 :].lstrip())
    except json.JSONDecodeError as exc:
        raise ValueError("MBOS archive component is invalid JSON") from exc
    return _mapping(value, "MBOS archive component")


def _title_period(title: object, year: object) -> str:
    text = _required_text(title, "archive title")
    year_text = _required_text(year, "archive year")
    match = re.match(r"(?P<month>[A-Za-z]+)\s+(?P<year>\d{4})$", text)
    if match is None or match.group("year") != year_text:
        raise ValueError("MBOS archive title differs from its year")
    month = _MONTH_NUMBER.get(match.group("month").lower())
    if month is None:
        raise ValueError("MBOS archive month is unsupported")
    return f"{int(year_text):04d}-{month:02d}"


def _modern_index_entries(
    content: bytes,
) -> tuple[PhiladelphiaFedMbosIndexEntryV1, ...]:
    payload = _embedded_archive_payload(content)
    groups = _sequence(payload.get("results"), "archive result groups")
    found: dict[str, PhiladelphiaFedMbosIndexEntryV1] = {}
    for group in groups:
        result_group = _mapping(group, "archive result group")
        for raw_result in _sequence(
            result_group.get("results"), "archive results"
        ):
            result = _mapping(raw_result, "archive result")
            attributes = _mapping(
                result.get("attributes"), "archive attributes"
            )
            period = _title_period(
                attributes.get("title"), attributes.get("year")
            )
            if period < "2008-01":
                continue
            pdf_uri: str | None = None
            page_uri: str | None = None
            for raw_file in _sequence(attributes.get("files"), "archive files"):
                file_data = _mapping(raw_file, "archive file")
                icon = _mapping(file_data.get("icon"), "archive file icon")
                kind = str(icon.get("text", "")).lower()
                uri = _normalize_uri(
                    urljoin(MBOS_INDEX_URI, str(file_data.get("url", "")))
                )
                if kind == "pdf":
                    if pdf_uri is not None:
                        raise ValueError("MBOS archive repeats a release PDF")
                    pdf_uri = uri
                elif kind == "page":
                    if page_uri is not None:
                        raise ValueError("MBOS archive repeats a release page")
                    page_uri = uri
            if pdf_uri is None:
                raise ValueError("MBOS archive entry lacks its release PDF")
            if period in found:
                raise ValueError("MBOS archive repeats a reference period")
            found[period] = PhiladelphiaFedMbosIndexEntryV1(
                reference_period=period,
                title=str(attributes.get("title", "")),
                artifact_uri=pdf_uri,
                archive_member=None,
                release_page_uri=page_uri,
            )
    if not found:
        raise ValueError("MBOS archive contains no 2008-present releases")
    periods = tuple(sorted(found))
    if periods != _periods("2008-01", periods[-1]):
        raise ValueError("MBOS modern archive is not continuous")
    return tuple(found[item] for item in periods)


def _legacy_entries(
    zip_content: bytes,
) -> tuple[PhiladelphiaFedMbosIndexEntryV1, ...]:
    try:
        with ZipFile(BytesIO(zip_content)) as archive:
            names = set(archive.namelist())
    except (BadZipFile, OSError) as exc:
        raise ValueError("MBOS bulk archive is not a readable ZIP") from exc
    entries: list[PhiladelphiaFedMbosIndexEntryV1] = []
    for period in _periods(MBOS_PREDECESSOR_PERIOD, "2007-12"):
        year, month = period.split("-")
        member = f"{year}/bos{month}{year[2:]}.pdf"
        if member not in names:
            raise ValueError(f"MBOS bulk archive omits {member}")
        entries.append(
            PhiladelphiaFedMbosIndexEntryV1(
                reference_period=period,
                title=f"{calendar.month_name[int(month)]} {year}",
                artifact_uri=MBOS_BULK_ARCHIVE_URI,
                archive_member=member,
                release_page_uri=None,
            )
        )
    return tuple(entries)


def parse_philadelphia_fed_mbos_release_index(
    archive_snapshot: OfficialRawSnapshotV1,
    bulk_snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> PhiladelphiaFedMbosReleaseIndexV1:
    """Join the live 2008+ index to the complete legacy ZIP."""
    as_of = _iso_date(as_of_date, "as_of_date")
    _validate_snapshot(
        archive_snapshot,
        uri=MBOS_INDEX_URI,
        source_format=OfficialSourceFormat.HTML,
    )
    _validate_snapshot(
        bulk_snapshot,
        uri=MBOS_BULK_ARCHIVE_URI,
        source_format=OfficialSourceFormat.ARCHIVE,
    )
    modern = tuple(
        item
        for item in _modern_index_entries(archive_snapshot.content)
        if item.reference_period <= as_of[:7]
    )
    if not modern:
        raise ValueError("MBOS has no modern releases at the as-of boundary")
    entries = (*_legacy_entries(bulk_snapshot.content), *modern)
    return PhiladelphiaFedMbosReleaseIndexV1(
        as_of_date=as_of,
        archive_index=_snapshot_artifact(
            archive_snapshot, "archive-index", source_format="html"
        ),
        bulk_archive=_snapshot_artifact(
            bulk_snapshot, "bulk-archive", source_format="archive"
        ),
        entries=entries,
    )


def build_philadelphia_fed_mbos_release_requests(
    registry: OfficialSourceRegistryV1,
    release_index: PhiladelphiaFedMbosReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan every direct 2008+ PDF and 2026 HTML report."""
    if not isinstance(release_index, PhiladelphiaFedMbosReleaseIndexV1):
        raise TypeError("MBOS release requests require a v1 release index")
    requests: list[OfficialSourceRequestV1] = []
    for entry in release_index.entries:
        if entry.archive_member is None:
            requests.append(
                _request(
                    registry,
                    entry.artifact_uri,
                    OfficialSourceFormat.PDF,
                    as_of_date=release_index.as_of_date,
                )
            )
        if entry.release_page_uri is not None:
            requests.append(
                _request(
                    registry,
                    entry.release_page_uri,
                    OfficialSourceFormat.HTML,
                    as_of_date=release_index.as_of_date,
                )
            )
    uris = tuple(item.uri for item in requests)
    if len(uris) != len(set(uris)):
        raise ValueError("MBOS direct request inventory repeats a URI")
    return tuple(requests)


def _normalize_pdf_text(value: str) -> str:
    normalized = (
        value.replace("\xa0", " ")
        .replace("‐", "-")
        .replace("–", "-")
        .replace("−", "-")
        .replace("—", "-")
        .replace("\uf02d", "-")
    )
    return re.sub(r"(?<=\d)\s*\.\s*(?=\d)", ".", normalized)


def _pdf_text(content: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(content), strict=False)
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:
        raise ValueError("MBOS release PDF cannot be read") from exc
    return _normalize_pdf_text(text)


def _table_tokens(text: str) -> tuple[str, ...]:
    candidates: list[tuple[str, ...]] = []
    pattern = re.compile(
        r"(?:What\s*is\s+your\s+evaluation|Whatis\s+your\s+evaluation)"
        r".{0,3000}?(?:Company\s*Business|New\s+Orders)",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(text):
        values = tuple(_NUMBER_RE.findall(match.group(0)))
        if len(values) == 9 and values[0] == "1":
            values = values[1:]
        if len(values) in {8, 10} and values not in candidates:
            candidates.append(values)
    if len(candidates) != 1:
        raise ValueError("MBOS general-activity table is ambiguous")
    return candidates[0]


def _canonical_index(value: float) -> str:
    return f"{value:.1f}"


def _parse_values(
    content: bytes,
    text: str,
    *,
    ocr: bool,
) -> tuple[str, str, tuple[str, ...]]:
    digest = hashlib.sha256(content).hexdigest()
    token_override = _TOKEN_OVERRIDES.get(digest)
    values: tuple[str, ...] = (
        _table_tokens(text) if token_override is None else token_override
    )
    override = _VALUE_OVERRIDES.get(digest)
    if override is not None:
        return override[0], override[1], values
    if len(values) == 8:
        if ocr:
            current = _recover_ocr_index(values[:4])
            future = _recover_ocr_index(values[4:])
        else:
            current, future = values[3], values[7]
    else:
        current, future = values[4], values[9]
    current_text, current_value = _lexical_number(current, "current index")
    future_text, future_value = _lexical_number(future, "future index")
    if len(values) == 8 and not ocr:
        current_expected = float(values[2]) - float(values[0])
        future_expected = float(values[6]) - float(values[4])
    elif len(values) == 10:
        current_expected = float(values[1]) - float(values[3])
        future_expected = float(values[6]) - float(values[8])
    else:
        current_expected = current_value
        future_expected = future_value
    if (
        abs(current_expected - current_value) > 0.3
        or abs(future_expected - future_value) > 0.3
    ):
        raise ValueError(
            "MBOS diffusion indexes fail their response-share check"
        )
    return current_text, future_text, values


def _ocr_number_candidates(value: str, *, signed: bool) -> tuple[float, ...]:
    """Return bounded decimal placements for one OCR-damaged table token."""
    raw = value.strip()
    if "." in raw:
        candidates = [float(raw)]
    else:
        integer = int(raw)
        candidates = [float(integer), integer / 10.0, integer / 100.0]
    if signed and not raw.startswith(("-", "+")):
        candidates.extend(-item for item in tuple(candidates) if item != 0)
    return tuple(
        dict.fromkeys(
            round(item, 2)
            for item in candidates
            if -100.0 <= item <= 100.0 and (signed or item >= 0)
        )
    )


def _recover_ocr_index(values: Sequence[str]) -> str:
    """Recover one OCR index from shares and the diffusion identity."""
    if len(values) != 4:
        raise ValueError("MBOS OCR table group must contain four values")
    candidates = (
        _ocr_number_candidates(values[0], signed=False),
        _ocr_number_candidates(values[1], signed=False),
        _ocr_number_candidates(values[2], signed=False),
        _ocr_number_candidates(values[3], signed=True),
    )
    solutions: list[tuple[float, float, float, float, float, float, float]] = []
    for decrease, unchanged, increase, index in product(*candidates):
        share_error = abs(decrease + unchanged + increase - 100.0)
        identity_error = abs(index - (increase - decrease))
        if share_error <= 15.0 and identity_error <= 1.0:
            solutions.append(
                (
                    identity_error,
                    share_error,
                    abs(index),
                    decrease,
                    unchanged,
                    increase,
                    index,
                )
            )
    if not solutions:
        raise ValueError("MBOS OCR index cannot satisfy its diffusion identity")
    best = min(solutions)
    matching = {
        round(index, 1)
        for identity_error, share_error, _, _, _, _, index in solutions
        if (identity_error, share_error) == best[:2]
    }
    if len(matching) != 1:
        raise ValueError("MBOS OCR index recovery is ambiguous")
    return _canonical_index(matching.pop())


def _release_metadata(
    *,
    period: str,
    pdf_text: str,
    release_page_content: bytes | None,
) -> tuple[str, str | None, EconomicTimePrecision, int | None, str]:
    if release_page_content is None:
        source = " ".join(pdf_text.split())
        locator = "release PDF Released/Release date and time line"
    else:
        try:
            source = release_page_content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError("MBOS release page is not UTF-8") from exc
        locator = "release HTML Released line"
    pattern = re.compile(
        r"(?:Released|Release\s+date\s+and\s+time)\s*:\s*"
        r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s*"
        r"(?:(?P<year>\d{4})[\s,\.]*?)?(?:at\s+)?"
        r"(?P<time>(?:12\s*:\s*00\s*)?noon|"
        r"\d{1,2}(?:\s*:\s*\d{2})?\s*(?:a\.?m\.?|p\.?m\.?))"
        r"\s*E\s*\.?\s*T\.?",
        re.IGNORECASE,
    )
    match = pattern.search(source)
    if match is None:
        if not (period <= "2005-05" or period == "2010-01"):
            raise ValueError(f"MBOS release metadata is missing for {period}")
        release_date = _third_thursday(period)
        inferred_time = "10:00" if period == "2010-01" else None
        return (
            release_date,
            inferred_time,
            EconomicTimePrecision.INFERRED_BOUNDED,
            None,
            "third-Thursday convention; occurrence lacks a release-time line",
        )
    month = _MONTH_NUMBER.get(match.group("month").lower())
    if month is None:
        raise ValueError("MBOS release month is unsupported")
    release_year = int(match.group("year") or period[:4])
    release_date = date(
        release_year, month, int(match.group("day"))
    ).isoformat()
    if release_date[:7] != period:
        raise ValueError("MBOS release date differs from reference month")
    raw_time = re.sub(r"\s+", "", match.group("time").lower()).replace(".", "")
    if "noon" in raw_time:
        local_time = "12:00"
    else:
        time_match = re.fullmatch(
            r"(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?(?P<meridiem>am|pm)",
            raw_time,
        )
        if time_match is None:
            raise ValueError("MBOS release time is invalid")
        hour = int(time_match.group("hour")) % 12
        if time_match.group("meridiem") == "pm":
            hour += 12
        minute = int(time_match.group("minute") or "00")
        local_time = f"{hour:02d}:{minute:02d}"
    released_at_ns = normalize_official_source_timestamp(
        f"{release_date}T{local_time}:00",
        MBOS_SOURCE_TIMEZONE,
        EconomicTimePrecision.EXACT_MINUTE,
    ).utc_ns
    return (
        release_date,
        local_time,
        EconomicTimePrecision.EXACT_MINUTE,
        released_at_ns,
        locator,
    )


def _legacy_members(content: bytes) -> Mapping[str, bytes]:
    try:
        with ZipFile(BytesIO(content)) as archive:
            return MappingProxyType(
                {
                    name: archive.read(name)
                    for name in archive.namelist()
                    if name.lower().endswith(".pdf")
                }
            )
    except (BadZipFile, OSError) as exc:
        raise ValueError("MBOS bulk archive cannot be replayed") from exc


def _release_bytes(
    entry: PhiladelphiaFedMbosIndexEntryV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    members: Mapping[str, bytes],
) -> bytes:
    if entry.archive_member is not None:
        try:
            content = members[entry.archive_member]
        except KeyError as exc:
            raise ValueError("MBOS indexed ZIP member is missing") from exc
    else:
        try:
            snapshot = snapshots_by_uri[entry.artifact_uri]
        except KeyError as exc:
            raise ValueError("MBOS direct PDF snapshot is missing") from exc
        _validate_snapshot(
            snapshot,
            uri=entry.artifact_uri,
            source_format=OfficialSourceFormat.PDF,
        )
        content = snapshot.content
    if not content.startswith(b"%PDF-"):
        raise ValueError("MBOS release artifact is not a PDF")
    return content


def _ocr_text(
    entry: PhiladelphiaFedMbosIndexEntryV1,
    content: bytes,
    ocr_by_locator: Mapping[str, PhiladelphiaFedMbosOcrEntryV1],
) -> tuple[str, PhiladelphiaFedMbosOcrEntryV1 | None]:
    if entry.reference_period > "2001-12":
        return _pdf_text(content), None
    try:
        ocr = ocr_by_locator[entry.source_locator]
    except KeyError as exc:
        raise ValueError("MBOS reviewed OCR entry is missing") from exc
    if ocr.content_sha256 != hashlib.sha256(content).hexdigest():
        raise ValueError("MBOS reviewed OCR differs from its source PDF")
    return _normalize_pdf_text("\n".join(ocr.page_texts)), ocr


def _artifact_for_release(
    entry: PhiladelphiaFedMbosIndexEntryV1,
    content: bytes,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    *,
    container_sha256: str,
    role: str,
) -> PhiladelphiaFedMbosArtifactV1:
    if entry.archive_member is not None:
        return _member_artifact(
            entry,
            content,
            container_sha256=container_sha256,
            role=role,
        )
    return _snapshot_artifact(snapshots_by_uri[entry.artifact_uri], role)


def _revised_previous_values(
    period: str,
    tokens: tuple[str, ...],
    previous_current: str,
    previous_future: str,
) -> tuple[str, str]:
    if len(tokens) == 10:
        current, _ = _lexical_number(tokens[0], "revised previous current")
        future, _ = _lexical_number(tokens[5], "revised previous future")
        return current, future
    if period in _JANUARY_REVISED_PREVIOUS:
        return _JANUARY_REVISED_PREVIOUS[period]
    return previous_current, previous_future


def build_philadelphia_fed_mbos_archive_manifest(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    release_index: PhiladelphiaFedMbosReleaseIndexV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    ocr_corpus: PhiladelphiaFedMbosOcrCorpusV1,
) -> PhiladelphiaFedMbosArchiveManifestV1:
    """Build one complete point-in-time MBOS manifest from retained evidence."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("MBOS manifest requires a v1 registry")
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("MBOS manifest requires a v1 U.S. profile")
    if not isinstance(release_index, PhiladelphiaFedMbosReleaseIndexV1):
        raise TypeError("MBOS manifest requires a v1 release index")
    if not isinstance(ocr_corpus, PhiladelphiaFedMbosOcrCorpusV1):
        raise TypeError("MBOS manifest requires a v1 reviewed OCR corpus")
    if profile.registry_id != registry.registry_id:
        raise ValueError("MBOS registry and profile identities differ")
    program = profile.by_key.get(MBOS_PROGRAM_KEY)
    if program is None or program.source_key != MBOS_SOURCE_KEY:
        raise ValueError("MBOS program source differs")

    required_uris = {
        MBOS_INDEX_URI,
        MBOS_BULK_ARCHIVE_URI,
        *(
            entry.artifact_uri
            for entry in release_index.entries
            if entry.archive_member is None
        ),
        *(
            entry.release_page_uri
            for entry in release_index.entries
            if entry.release_page_uri is not None
        ),
    }
    if set(snapshots_by_uri) != required_uris:
        raise ValueError("MBOS retained snapshot inventory differs")
    archive_snapshot = snapshots_by_uri[MBOS_INDEX_URI]
    bulk_snapshot = snapshots_by_uri[MBOS_BULK_ARCHIVE_URI]
    _validate_snapshot(
        archive_snapshot,
        uri=MBOS_INDEX_URI,
        source_format=OfficialSourceFormat.HTML,
    )
    _validate_snapshot(
        bulk_snapshot,
        uri=MBOS_BULK_ARCHIVE_URI,
        source_format=OfficialSourceFormat.ARCHIVE,
    )
    if (
        _snapshot_artifact(
            archive_snapshot, "archive-index", source_format="html"
        )
        != release_index.archive_index
        or _snapshot_artifact(
            bulk_snapshot, "bulk-archive", source_format="archive"
        )
        != release_index.bulk_archive
    ):
        raise ValueError("MBOS index artifacts changed after parsing")

    members = _legacy_members(bulk_snapshot.content)
    expected_ocr_locators = tuple(
        entry.source_locator
        for entry in release_index.entries
        if entry.reference_period <= "2001-12"
    )
    if tuple(sorted(expected_ocr_locators)) != tuple(
        item.source_locator for item in ocr_corpus.entries
    ):
        raise ValueError("MBOS reviewed OCR inventory differs")

    predecessor_entry = release_index.entries[0]
    predecessor_bytes = _release_bytes(
        predecessor_entry, snapshots_by_uri, members
    )
    predecessor_text, predecessor_ocr = _ocr_text(
        predecessor_entry, predecessor_bytes, ocr_corpus.by_locator
    )
    if predecessor_ocr is None:
        raise ValueError("MBOS predecessor requires reviewed OCR")
    predecessor_current, predecessor_future, _ = _parse_values(
        predecessor_bytes, predecessor_text, ocr=True
    )
    predecessor = _artifact_for_release(
        predecessor_entry,
        predecessor_bytes,
        snapshots_by_uri,
        container_sha256=bulk_snapshot.content_sha256,
        role="predecessor-pdf",
    )

    publications: list[PhiladelphiaFedMbosPublicationV1] = []
    previous_current = predecessor_current
    previous_future = predecessor_future
    for entry in release_index.entries[1:]:
        content = _release_bytes(entry, snapshots_by_uri, members)
        text, ocr = _ocr_text(entry, content, ocr_corpus.by_locator)
        current, future, tokens = _parse_values(
            content, text, ocr=ocr is not None
        )
        parser_era = (
            "ocr-pdf-eight-column"
            if ocr is not None
            else (
                "native-pdf-eight-column"
                if len(tokens) == 8
                else "native-pdf-ten-column"
            )
        )
        if (parser_era.endswith("eight-column")) != (len(tokens) == 8):
            raise ValueError("MBOS parser era differs from its table")

        page_artifact: PhiladelphiaFedMbosArtifactV1 | None = None
        page_content: bytes | None = None
        if entry.release_page_uri is not None:
            page_snapshot = snapshots_by_uri[entry.release_page_uri]
            _validate_snapshot(
                page_snapshot,
                uri=entry.release_page_uri,
                source_format=OfficialSourceFormat.HTML,
            )
            page_content = page_snapshot.content
            page_artifact = _snapshot_artifact(
                page_snapshot, "release-page", source_format="html"
            )
        release_date, release_time, precision, released_at_ns, locator = (
            _release_metadata(
                period=entry.reference_period,
                pdf_text=text,
                release_page_content=page_content,
            )
        )
        revised_current, revised_future = _revised_previous_values(
            entry.reference_period,
            tokens,
            previous_current,
            previous_future,
        )
        current_lexical, current_value = _lexical_number(
            current, "current index"
        )
        future_lexical, future_value = _lexical_number(future, "future index")
        previous_current_lexical, previous_current_value = _lexical_number(
            previous_current, "previous current"
        )
        previous_future_lexical, previous_future_value = _lexical_number(
            previous_future, "previous future"
        )
        revised_current_lexical, revised_current_value = _lexical_number(
            revised_current, "revised previous current"
        )
        revised_future_lexical, revised_future_value = _lexical_number(
            revised_future, "revised previous future"
        )
        revision_notice = (
            entry.reference_period.endswith("-01")
            or entry.reference_period == "2021-04"
        )
        if (
            revised_current_value != previous_current_value
            or revised_future_value != previous_future_value
        ) and not revision_notice:
            raise ValueError("MBOS unannounced revision differs")
        release_pdf = _artifact_for_release(
            entry,
            content,
            snapshots_by_uri,
            container_sha256=bulk_snapshot.content_sha256,
            role="release-pdf",
        )
        publications.append(
            PhiladelphiaFedMbosPublicationV1(
                reference_period=entry.reference_period,
                previous_reference_period=(
                    MBOS_PREDECESSOR_PERIOD
                    if entry.reference_period == MBOS_FIRST_PERIOD
                    else publications[-1].reference_period
                ),
                release_date=release_date,
                release_time_local=release_time,
                time_precision=precision,
                released_at_ns=released_at_ns,
                current_index_lexical=current_lexical,
                current_index=current_value,
                future_index_lexical=future_lexical,
                future_index=future_value,
                previous_current_lexical=previous_current_lexical,
                previous_current=previous_current_value,
                previous_future_lexical=previous_future_lexical,
                previous_future=previous_future_value,
                revised_previous_current_lexical=revised_current_lexical,
                revised_previous_current=revised_current_value,
                revised_previous_future_lexical=revised_future_lexical,
                revised_previous_future=revised_future_value,
                revision_notice=revision_notice,
                parser_era=parser_era,
                release_pdf=release_pdf,
                release_page=page_artifact,
                ocr_entry_id=None if ocr is None else ocr.entry_id,
                release_locator=locator,
                value_locator=(
                    "reviewed OCR Summary of Returns general-activity row"
                    if ocr is not None
                    else "release PDF Summary of Returns general-activity row"
                ),
                revision_locator=(
                    "release PDF previous-diffusion-index columns"
                    if len(tokens) == 10
                    else (
                        "release narrative annual-revision comparison"
                        if entry.reference_period in _JANUARY_REVISED_PREVIOUS
                        else "preceding occurrence initial vintage"
                    )
                ),
            )
        )
        previous_current = current_lexical
        previous_future = future_lexical

    all_snapshots = tuple(snapshots_by_uri.values())
    publication_pdf_bytes = predecessor.content_length + sum(
        item.release_pdf.content_length for item in publications
    )
    return PhiladelphiaFedMbosArchiveManifestV1(
        registry_id=registry.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        ocr_corpus_id=ocr_corpus.corpus_id,
        predecessor=predecessor,
        predecessor_current_lexical=predecessor_current,
        predecessor_current=float(predecessor_current),
        predecessor_future_lexical=predecessor_future,
        predecessor_future=float(predecessor_future),
        publications=tuple(publications),
        raw_artifact_count=len(all_snapshots),
        total_content_bytes=sum(len(item.content) for item in all_snapshots),
        publication_pdf_bytes=publication_pdf_bytes,
        exact_minute_count=sum(
            item.time_precision is EconomicTimePrecision.EXACT_MINUTE
            for item in publications
        ),
        inferred_bounded_count=sum(
            item.time_precision is EconomicTimePrecision.INFERRED_BOUNDED
            for item in publications
        ),
        forecast_occurrence_count=len(publications),
        revision_record_count=sum(
            item.revision_record_count for item in publications
        ),
        revision_publication_count=sum(
            item.revision_record_count > 0 for item in publications
        ),
        ocr_publication_count=sum(
            item.ocr_entry_id is not None for item in publications
        ),
    )


def replay_philadelphia_fed_mbos_archive(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    ocr_corpus: PhiladelphiaFedMbosOcrCorpusV1,
    expected: PhiladelphiaFedMbosArchiveManifestV1,
) -> PhiladelphiaFedMbosArchiveManifestV1:
    """Rebuild and compare the complete retained MBOS corpus."""
    rebuilt_index = parse_philadelphia_fed_mbos_release_index(
        snapshots_by_uri[MBOS_INDEX_URI],
        snapshots_by_uri[MBOS_BULK_ARCHIVE_URI],
        as_of_date=expected.release_index.as_of_date,
    )
    rebuilt = build_philadelphia_fed_mbos_archive_manifest(
        registry, profile, rebuilt_index, snapshots_by_uri, ocr_corpus
    )
    if rebuilt != expected:
        raise ValueError("MBOS retained-corpus replay differs")
    return rebuilt


def philadelphia_fed_mbos_coverage_from_manifest(
    manifest: PhiladelphiaFedMbosArchiveManifestV1,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete monthly occurrence and native-outlook coverage."""
    if not isinstance(manifest, PhiladelphiaFedMbosArchiveManifestV1):
        raise TypeError("MBOS coverage requires a v1 manifest")
    count = len(manifest.publications)
    artifacts = (
        manifest.predecessor,
        *(item.release_pdf for item in manifest.publications),
    )
    return UnitedStatesProgramCoverageV1(
        program_key=MBOS_PROGRAM_KEY,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=manifest.release_index.as_of_date,
        expected_occurrence_count=count,
        schedule_count=count,
        initial_actual_count=count,
        previous_as_known_count=count,
        revision_count=manifest.revision_record_count,
        exact_minute_count=manifest.exact_minute_count,
        forecast_count=manifest.forecast_occurrence_count,
        artifact_sha256s=tuple(item.content_sha256 for item in artifacts),
        gap_reasons=(),
        notes=(
            "Each release preserves current and native six-month-future diffusion indexes as separate concepts.",
            "The preceding occurrence supplies previous-as-known; annual and April 2021 seasonal revisions remain explicit.",
            "Exact source-authored times begin in June 2005; earlier occurrences and January 2010 remain bounded rather than invented.",
            "Reviewed OCR is hash-bound to the image-only December 1999 through December 2001 reports.",
            "MBOS measures Third District manufacturing sentiment, not national output or monthly event consensus.",
        ),
    )


def packaged_philadelphia_fed_mbos_manifest_path() -> Path:
    return Path(__file__).with_name("assets") / "us_mbos_archive_v1.json"


def packaged_philadelphia_fed_mbos_index_path() -> Path:
    return (
        Path(__file__).with_name("assets") / "us_mbos_release_index_v1.html.b64"
    )


def packaged_philadelphia_fed_mbos_ocr_path() -> Path:
    return Path(__file__).with_name("assets") / "us_mbos_ocr_v1.json"


def load_packaged_philadelphia_fed_mbos_archive_manifest() -> (
    PhiladelphiaFedMbosArchiveManifestV1
):
    """Load and validate the compact packaged MBOS manifest."""
    return PhiladelphiaFedMbosArchiveManifestV1.from_json(
        packaged_philadelphia_fed_mbos_manifest_path().read_text(
            encoding="utf-8"
        )
    )


def load_packaged_philadelphia_fed_mbos_release_index() -> bytes:
    """Load and validate the exact packaged official archive index receipt."""
    try:
        content = base64.b64decode(
            packaged_philadelphia_fed_mbos_index_path()
            .read_text(encoding="ascii")
            .strip(),
            validate=True,
        )
    except (OSError, UnicodeError, ValueError) as exc:
        raise ValueError("packaged MBOS archive index is invalid") from exc
    artifact = (
        load_packaged_philadelphia_fed_mbos_archive_manifest().release_index.archive_index
    )
    if (
        len(content) != artifact.content_length
        or hashlib.sha256(content).hexdigest() != artifact.content_sha256
    ):
        raise ValueError("packaged MBOS archive index differs")
    return content


def load_packaged_philadelphia_fed_mbos_ocr_corpus() -> (
    PhiladelphiaFedMbosOcrCorpusV1
):
    """Load and identity-check the packaged reviewed OCR corpus."""
    corpus = PhiladelphiaFedMbosOcrCorpusV1.from_json(
        packaged_philadelphia_fed_mbos_ocr_path().read_text(encoding="utf-8")
    )
    manifest = load_packaged_philadelphia_fed_mbos_archive_manifest()
    if corpus.corpus_id != manifest.ocr_corpus_id:
        raise ValueError("packaged MBOS OCR corpus differs")
    return corpus


__all__ = [
    "MAX_MBOS_ARTIFACT_BYTES",
    "MAX_MBOS_OCR_TEXT_BYTES",
    "MAX_MBOS_RELEASES",
    "MAX_MBOS_TOTAL_BYTES",
    "MBOS_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "MBOS_ARCHIVE_URI",
    "MBOS_ARTIFACT_SCHEMA_VERSION",
    "MBOS_BULK_ARCHIVE_URI",
    "MBOS_FIRST_PERIOD",
    "MBOS_INDEX_ENTRY_SCHEMA_VERSION",
    "MBOS_INDEX_URI",
    "MBOS_LATEST_PACKAGED_PERIOD",
    "MBOS_OCR_CORPUS_SCHEMA_VERSION",
    "MBOS_OCR_ENTRY_SCHEMA_VERSION",
    "MBOS_PREDECESSOR_PERIOD",
    "MBOS_PROGRAM_KEY",
    "MBOS_PUBLICATION_SCHEMA_VERSION",
    "MBOS_RELEASE_INDEX_SCHEMA_VERSION",
    "MBOS_SOURCE_KEY",
    "PhiladelphiaFedMbosArchiveManifestV1",
    "PhiladelphiaFedMbosArtifactV1",
    "PhiladelphiaFedMbosIndexEntryV1",
    "PhiladelphiaFedMbosOcrCorpusV1",
    "PhiladelphiaFedMbosOcrEntryV1",
    "PhiladelphiaFedMbosPublicationV1",
    "PhiladelphiaFedMbosReleaseIndexV1",
    "build_philadelphia_fed_mbos_archive_manifest",
    "build_philadelphia_fed_mbos_release_requests",
    "build_philadelphia_fed_mbos_support_requests",
    "load_packaged_philadelphia_fed_mbos_archive_manifest",
    "load_packaged_philadelphia_fed_mbos_ocr_corpus",
    "load_packaged_philadelphia_fed_mbos_release_index",
    "packaged_philadelphia_fed_mbos_index_path",
    "packaged_philadelphia_fed_mbos_manifest_path",
    "packaged_philadelphia_fed_mbos_ocr_path",
    "parse_philadelphia_fed_mbos_release_index",
    "philadelphia_fed_mbos_coverage_from_manifest",
    "replay_philadelphia_fed_mbos_archive",
]
