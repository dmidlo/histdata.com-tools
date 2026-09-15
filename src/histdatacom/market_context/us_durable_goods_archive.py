"""Occurrence-specific Census Advance Durable Goods archive evidence.

The Census M3 archive contains image-only early reports, one unrecoverable
November 2009 advance report, a broken July 2005 archive link, annual
historical revisions, classification changes, and two government-shutdown
sequences.  This module retains those facts and reconstructs the source-
reported month-over-month changes for total durable-goods new orders and
nondefense capital-goods new orders excluding aircraft.
"""

from __future__ import annotations

import base64
import calendar
import hashlib
import html
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import ROUND_HALF_UP, Decimal
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)
from histdatacom.market_context.us_backfill import (
    US_BACKFILL_START_DATE,
    UnitedStatesBackfillProfileV1,
    UnitedStatesCoverageGapReason,
    UnitedStatesProgramCoverageV1,
)
from histdatacom.runtime_contracts import JSONValue

DURABLE_INDEX_ENTRY_SCHEMA_VERSION = "histdatacom.durable-index-entry.v1"
DURABLE_INDEX_SCHEMA_VERSION = "histdatacom.durable-index.v1"
DURABLE_OCR_ENTRY_SCHEMA_VERSION = "histdatacom.durable-ocr-entry.v1"
DURABLE_OCR_CORPUS_SCHEMA_VERSION = "histdatacom.durable-ocr-corpus.v1"
DURABLE_MEASURE_SCHEMA_VERSION = "histdatacom.durable-measure.v1"
DURABLE_PUBLICATION_SCHEMA_VERSION = "histdatacom.durable-publication.v1"
DURABLE_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.durable-archive-manifest.v1"
)
DURABLE_INDEX_ENVELOPE_SCHEMA_VERSION = "histdatacom.durable-index-envelope.v1"

DURABLE_SOURCE_KEY = "us.census.durable-goods"
DURABLE_PROGRAM_KEY = DURABLE_SOURCE_KEY
DURABLE_INDEX_URI = (
    "https://www.census.gov/manufacturing/m3/adv/historical_data/index.html"
)
DURABLE_RELEASE_SCHEDULE_URI = (
    "https://www.census.gov/manufacturing/m3/release_schedule.html"
)
DURABLE_PREDECESSOR_REFERENCE_PERIOD = "1999-12"
DURABLE_PREDECESSOR_URI = (
    "https://www.census.gov/manufacturing/m3/historical_data/"
    "pressreleases/adv/1999/dec99adv.pdf"
)
DURABLE_FIRST_REFERENCE_PERIOD = "2000-01"
DURABLE_OFFICIALLY_UNAVAILABLE_PERIOD = "2009-11"
DURABLE_HEADLINE_ORDERS = "durable-goods-new-orders"
DURABLE_CORE_CAPITAL_GOODS = (
    "nondefense-capital-goods-new-orders-excluding-aircraft"
)
DURABLE_MEASURE_KEYS = (DURABLE_HEADLINE_ORDERS, DURABLE_CORE_CAPITAL_GOODS)

DURABLE_JULY_2005_LISTED_URI = (
    "https://www.census.gov/manufacturing/m3/historical_data/"
    "pressreleases/adv/2005/jul05adv.pdf"
)
DURABLE_JULY_2005_ARTIFACT_URI = (
    "https://www2.census.gov/indicators/m3/2005/jul05adv.pdf.pdf"
)

DURABLE_CORE_SOURCE_UNAVAILABLE_PERIODS = (
    "2001-04",
    "2001-05",
    "2001-06",
    "2001-07",
    "2001-08",
    "2001-09",
    "2001-10",
    "2001-11",
    "2001-12",
    "2002-01",
    "2002-02",
    "2002-03",
    "2002-04",
)

DURABLE_SHUTDOWN_DELAYED_PERIODS = (
    "2018-12",
    "2019-01",
    "2019-02",
    "2025-09",
    "2025-10",
    "2025-11",
    "2025-12",
    "2026-01",
    "2026-02",
)

MAX_DURABLE_RELEASES = 512
MAX_DURABLE_ARTIFACT_BYTES = 32 * 1024 * 1024
MAX_DURABLE_INDEX_BYTES = 4 * 1024 * 1024
MAX_DURABLE_OCR_TEXT_BYTES = 2 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_YEAR_MONTH_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")
_HREF_RE = re.compile(r"\bhref=[\"'](?P<uri>[^\"']+)[\"']", re.IGNORECASE)
_ARCHIVE_LINK_RE = re.compile(
    r"/adv/(?P<year>\d{4})/(?P<month>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)"
    r"(?P<short_year>\d{2})adv\.pdf$",
    re.IGNORECASE,
)
_DATE_RE = re.compile(
    r"\b(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(?P<day>\d{1,2})\s*,\s*"
    r"(?P<year>\d{4})\b",
    re.IGNORECASE,
)
_TIME_RE = re.compile(
    r"\b(?P<hour>8|10)\s*:\s*(?P<minute>30|00)\s*"
    r"(?:A\.?\s*M\.?)?\s*(?P<zone>ET|EST|EDT)\b",
    re.IGNORECASE,
)
_NUMBER_RE = re.compile(r"[+-]?\d[\d,]*(?:\.\s*\d+)?")
_HYPHEN_TRANSLATION = str.maketrans(
    {"−": "-", "‐": "-", "‑": "-", "‒": "-", "–": "-", "—": "-"}
)
_MONTH_NUMBER = {
    name.lower(): number
    for number, name in enumerate(calendar.month_abbr)
    if name
}
_FULL_MONTH_NUMBER = {
    name.lower(): number
    for number, name in enumerate(calendar.month_name)
    if name
}

# Pypdf separates letters in exactly three 2015 release headers.  The entries
# are bound to the immutable official PDF hashes so replacement bytes fail.
_RELEASE_METADATA_OVERRIDES = MappingProxyType(
    {
        "dadd24fd6e5b5d9583c0817970d9a1b243c3690724f789fd26a0e20ebe2d003d": (
            "2015-07-27",
            8,
            30,
            "EDT",
        ),
        "949ec018fbe1f075ccb526fb9b7eb1779a0fd99b3d774dd85a80a345cd32445c": (
            "2015-09-24",
            8,
            30,
            "EDT",
        ),
        "3b6b0a7aa53af385716c7aa6eefbd4f346de98bbc2fa47153aa37709894dfa11": (
            "2015-10-27",
            8,
            30,
            "EDT",
        ),
        # The official notice has no timestamp.  The December release's
        # embedded 2010 schedule records the missed occurrence explicitly.
        "574ad7d65ca6f55426ab69a04f6a4860009d099d1fa98035ff143c7d7236046e": (
            "2009-12-24",
            8,
            30,
            "EST",
        ),
    }
)

# Tesseract text stays untouched in the packaged corpus.  These reviewed rows
# correct only cells visibly misread on page 2 and are PDF-hash-bound.
_OCR_ROW_OVERRIDES = MappingProxyType(
    {
        (
            "bd124e7360100f0983be840b3622121770222d63b85cb902de27ad4e57972088",
            DURABLE_HEADLINE_ORDERS,
        ): ((216_115, 209_978, 241_748), ("2.9", "-13.1")),
        (
            "7be79c6e4a7f26e9ed61a80ed8b4a1d8827e357e45824a7fd3bcf144b0fd587e",
            DURABLE_HEADLINE_ORDERS,
        ): ((208_448, 213_344, 218_167), ("-2.3", "-2.2")),
        (
            "7be79c6e4a7f26e9ed61a80ed8b4a1d8827e357e45824a7fd3bcf144b0fd587e",
            DURABLE_CORE_CAPITAL_GOODS,
        ): ((50_037, 54_065, 51_294), ("-7.5", "5.4")),
        (
            "cf99b2c9f58b6e4b526bf292e191654772818ceddf303231aa173840d435c154",
            DURABLE_HEADLINE_ORDERS,
        ): ((199_199, 199_588, 215_289), ("-0.2", "-7.3")),
        (
            "f2c1b65e470303000a02f6b57a332af406a067ca7d7bf16bdd3d3b0bef64b6f1",
            DURABLE_HEADLINE_ORDERS,
        ): ((179_055, 174_467, 172_952), ("2.6", "0.9")),
        (
            "c1804f9141d38148e57975c716107d3a2fa88def65f7a446f079b3a594a07598",
            DURABLE_CORE_CAPITAL_GOODS,
        ): ((51_176, 50_693, 54_272), ("1.0", "-6.6")),
        (
            "25956fb61e44378ca2029e2b94ab092b040291bd6bf845597cef5b82f2c930cf",
            DURABLE_CORE_CAPITAL_GOODS,
        ): ((53_327, 54_453, 53_064), ("-2.1", "2.6")),
        (
            "6ba75cc23eca4073a93543076363f5764efad88dab392fc0f2638e615f534c85",
            DURABLE_HEADLINE_ORDERS,
        ): ((210_943, 206_214, 220_651), ("2.3", "-6.5")),
    }
)


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


def _https_uri(value: object, name: str) -> str:
    result = _required_text(value, name)
    parsed = urlparse(result)
    if parsed.scheme != "https" or not parsed.netloc or parsed.fragment:
        raise ValueError(f"{name} is not an absolute HTTPS URI")
    return result


def _positive_int(value: object, name: str, maximum: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    if not 0 < value <= maximum:
        raise ValueError(f"{name} is outside its bound")
    return value


def _count(value: object, name: str, maximum: int = 100_000) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside its bound")
    return value


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _next_month(period: str) -> str:
    year, month = (int(item) for item in period.split("-"))
    if month == 12:
        return f"{year + 1:04d}-01"
    return f"{year:04d}-{month + 1:02d}"


def _period_from_link(match: re.Match[str]) -> str:
    year = int(match.group("year"))
    if year % 100 != int(match.group("short_year")):
        raise ValueError("durable-goods archive link year differs")
    month = _MONTH_NUMBER[match.group("month").lower()]
    return f"{year:04d}-{month:02d}"


def _canonical_percentage(value: Decimal) -> tuple[str, float]:
    rounded = value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    if rounded == 0:
        rounded = Decimal("0.0")
    lexical = format(rounded, ".1f")
    return lexical, float(rounded)


def _percentage(value: object, name: str) -> tuple[str, float]:
    text = _required_text(value, name)
    if re.fullmatch(r"-?(?:0|[1-9]\d*)\.\d", text) is None:
        raise ValueError(f"{name} must have one decimal place")
    numeric = float(text)
    if not math.isfinite(numeric) or text == "-0.0":
        raise ValueError(f"{name} is invalid")
    return text, numeric


@dataclass(frozen=True, slots=True)
class CensusDurableReleaseIndexEntryV1:
    """One official advance-report link and its bounded correction state."""

    reference_period: str
    listed_uri: str
    artifact_uri: str
    link_corrected: bool
    entry_id: str = ""
    schema_version: str = DURABLE_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != DURABLE_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported durable-goods index-entry schema")
        period = _year_month(self.reference_period, "reference_period")
        listed = _https_uri(self.listed_uri, "listed_uri")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        if not isinstance(self.link_corrected, bool):
            raise TypeError("link_corrected must be boolean")
        corrected = listed == DURABLE_JULY_2005_LISTED_URI
        expected_artifact = (
            DURABLE_JULY_2005_ARTIFACT_URI if corrected else listed
        )
        if artifact != expected_artifact or self.link_corrected != corrected:
            raise ValueError("durable-goods link correction differs")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "listed_uri", listed)
        object.__setattr__(self, "artifact_uri", artifact)
        expected = _stable_id("durable-index-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("durable-goods index-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "listed_uri": self.listed_uri,
            "artifact_uri": self.artifact_uri,
            "link_corrected": self.link_corrected,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusDurableReleaseIndexEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            listed_uri=str(data.get("listed_uri", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            link_corrected=cast(bool, data.get("link_corrected")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusDurableReleaseIndexV1:
    """Hash-bound enumeration of the official advance-report archive."""

    source_uri: str
    content_sha256: str
    content_length: int
    as_of_date: str
    releases: tuple[CensusDurableReleaseIndexEntryV1, ...]
    index_id: str = ""
    schema_version: str = DURABLE_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != DURABLE_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported durable-goods index schema")
        if _https_uri(self.source_uri, "source_uri") != DURABLE_INDEX_URI:
            raise ValueError("durable-goods index URI differs")
        sha = _sha256(self.content_sha256, "content_sha256")
        _positive_int(
            self.content_length, "content_length", MAX_DURABLE_INDEX_BYTES
        )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        releases = tuple(self.releases)
        if (
            not releases
            or len(releases) > MAX_DURABLE_RELEASES
            or any(
                not isinstance(item, CensusDurableReleaseIndexEntryV1)
                for item in releases
            )
        ):
            raise TypeError("durable-goods index releases are invalid")
        periods = tuple(item.reference_period for item in releases)
        expected: list[str] = []
        period = DURABLE_FIRST_REFERENCE_PERIOD
        while period <= periods[-1]:
            expected.append(period)
            period = _next_month(period)
        if periods != tuple(expected):
            raise ValueError("durable-goods index has a period gap")
        if sum(item.link_corrected for item in releases) != 1:
            raise ValueError("durable-goods index correction count differs")
        object.__setattr__(self, "content_sha256", sha)
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "releases", releases)
        expected_id = _stable_id("durable-index", self.identity_payload())
        if self.index_id and self.index_id != expected_id:
            raise ValueError("durable-goods index identity differs")
        object.__setattr__(self, "index_id", expected_id)

    @property
    def by_period(self) -> Mapping[str, CensusDurableReleaseIndexEntryV1]:
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
    def from_dict(cls, data: Mapping[str, Any]) -> CensusDurableReleaseIndexV1:
        return cls(
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            as_of_date=str(data.get("as_of_date", "")),
            releases=tuple(
                CensusDurableReleaseIndexEntryV1.from_dict(
                    _mapping(item, "durable-goods index entry")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusDurableOcrEntryV1:
    """Reviewed two-page OCR bound to one scan-only official PDF."""

    artifact_uri: str
    content_sha256: str
    page_texts: tuple[str, ...]
    entry_id: str = ""
    schema_version: str = DURABLE_OCR_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != DURABLE_OCR_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported durable-goods OCR-entry schema")
        uri = _https_uri(self.artifact_uri, "artifact_uri")
        sha = _sha256(self.content_sha256, "content_sha256")
        pages = tuple(
            _required_text(item, "page_texts") for item in self.page_texts
        )
        if len(pages) != 2:
            raise ValueError("durable-goods OCR requires pages 1 and 2")
        if sum(len(item.encode("utf-8")) for item in pages) > (
            MAX_DURABLE_OCR_TEXT_BYTES
        ):
            raise ValueError("durable-goods OCR text exceeds its bound")
        object.__setattr__(self, "artifact_uri", uri)
        object.__setattr__(self, "content_sha256", sha)
        object.__setattr__(self, "page_texts", pages)
        expected = _stable_id("durable-ocr-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("durable-goods OCR-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "artifact_uri": self.artifact_uri,
            "content_sha256": self.content_sha256,
            "page_texts": list(self.page_texts),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CensusDurableOcrEntryV1:
        return cls(
            artifact_uri=str(data.get("artifact_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            page_texts=tuple(
                str(item)
                for item in _sequence(data.get("page_texts"), "page_texts")
            ),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusDurableOcrCorpusV1:
    """Deterministic page-aware OCR settings and reviewed text."""

    engine: str
    engine_version: str
    render_dpi: int
    page_modes: tuple[int, ...]
    entries: tuple[CensusDurableOcrEntryV1, ...]
    corpus_id: str = ""
    schema_version: str = DURABLE_OCR_CORPUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != DURABLE_OCR_CORPUS_SCHEMA_VERSION:
            raise ValueError("unsupported durable-goods OCR-corpus schema")
        if _required_text(self.engine, "engine") != "tesseract":
            raise ValueError("durable-goods OCR engine differs")
        version = _required_text(self.engine_version, "engine_version")
        if self.render_dpi != 300 or self.page_modes != (6, 6):
            raise ValueError("durable-goods OCR settings differ")
        entries = tuple(self.entries)
        uris = tuple(item.artifact_uri for item in entries)
        if (
            len(entries) != 37
            or uris != tuple(sorted(set(uris)))
            or len({item.content_sha256 for item in entries}) != len(entries)
        ):
            raise ValueError("durable-goods OCR inventory differs")
        object.__setattr__(self, "engine_version", version)
        object.__setattr__(self, "entries", entries)
        expected = _stable_id("durable-ocr-corpus", self.identity_payload())
        if self.corpus_id and self.corpus_id != expected:
            raise ValueError("durable-goods OCR-corpus identity differs")
        object.__setattr__(self, "corpus_id", expected)

    @property
    def by_uri(self) -> Mapping[str, CensusDurableOcrEntryV1]:
        return MappingProxyType(
            {item.artifact_uri: item for item in self.entries}
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
    def from_dict(cls, data: Mapping[str, Any]) -> CensusDurableOcrCorpusV1:
        return cls(
            engine=str(data.get("engine", "")),
            engine_version=str(data.get("engine_version", "")),
            render_dpi=cast(int, data.get("render_dpi")),
            page_modes=tuple(
                cast(int, item)
                for item in _sequence(data.get("page_modes"), "page_modes")
            ),
            entries=tuple(
                CensusDurableOcrEntryV1.from_dict(
                    _mapping(item, "durable-goods OCR entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            corpus_id=str(data.get("corpus_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> CensusDurableOcrCorpusV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "durable-goods OCR corpus is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(payload, "durable-goods OCR corpus"))


@dataclass(frozen=True, slots=True)
class CensusDurableMeasureV1:
    """One occurrence-specific rate and its previous-vintage lineage."""

    measure_key: str
    reference_period: str
    actual_value: float
    actual_lexical: str
    revised_previous_value: float
    revised_previous_lexical: str
    current_level_millions: int
    previous_level_millions: int
    two_months_ago_level_millions: int
    previous_as_known_value: float | None
    previous_as_known_lexical: str | None
    previous_artifact_uri: str | None
    revision_comparable: bool
    comparison_basis: str
    measure_id: str = ""
    schema_version: str = DURABLE_MEASURE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != DURABLE_MEASURE_SCHEMA_VERSION:
            raise ValueError("unsupported durable-goods measure schema")
        key = _required_text(self.measure_key, "measure_key")
        if key not in DURABLE_MEASURE_KEYS:
            raise ValueError("unsupported durable-goods measure key")
        period = _year_month(self.reference_period, "reference_period")
        actual_text, actual = _percentage(self.actual_lexical, "actual_lexical")
        revised_text, revised = _percentage(
            self.revised_previous_lexical, "revised_previous_lexical"
        )
        if (
            self.actual_value != actual
            or self.revised_previous_value != revised
        ):
            raise ValueError("durable-goods lexical and numeric rates differ")
        for name in (
            "current_level_millions",
            "previous_level_millions",
            "two_months_ago_level_millions",
        ):
            _positive_int(getattr(self, name), name, 10_000_000)
        previous_text = _optional_text(self.previous_as_known_lexical)
        previous_uri = _optional_text(self.previous_artifact_uri)
        if previous_text is None:
            if (
                self.previous_as_known_value is not None
                or previous_uri is not None
            ):
                raise ValueError("durable-goods previous state is incomplete")
            if self.revision_comparable:
                raise ValueError("missing previous state cannot be comparable")
        else:
            checked_text, previous = _percentage(
                previous_text, "previous_as_known_lexical"
            )
            if self.previous_as_known_value != previous or previous_uri is None:
                raise ValueError("durable-goods previous state differs")
            previous_text = checked_text
            previous_uri = _https_uri(previous_uri, "previous_artifact_uri")
        if not isinstance(self.revision_comparable, bool):
            raise TypeError("revision_comparable must be boolean")
        basis = _required_text(self.comparison_basis, "comparison_basis")
        if basis not in {
            "previous-advance-release",
            "prior-core-measure-unavailable",
            "intervening-historical-correction",
        }:
            raise ValueError("durable-goods comparison basis differs")
        object.__setattr__(self, "measure_key", key)
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "actual_lexical", actual_text)
        object.__setattr__(self, "revised_previous_lexical", revised_text)
        object.__setattr__(self, "previous_as_known_lexical", previous_text)
        object.__setattr__(self, "previous_artifact_uri", previous_uri)
        object.__setattr__(self, "comparison_basis", basis)
        expected = _stable_id("durable-measure", self.identity_payload())
        if self.measure_id and self.measure_id != expected:
            raise ValueError("durable-goods measure identity differs")
        object.__setattr__(self, "measure_id", expected)

    @property
    def previous_was_revised(self) -> bool:
        return bool(
            self.revision_comparable
            and self.previous_as_known_value != self.revised_previous_value
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "measure_key": self.measure_key,
            "reference_period": self.reference_period,
            "actual_value": self.actual_value,
            "actual_lexical": self.actual_lexical,
            "revised_previous_value": self.revised_previous_value,
            "revised_previous_lexical": self.revised_previous_lexical,
            "current_level_millions": self.current_level_millions,
            "previous_level_millions": self.previous_level_millions,
            "two_months_ago_level_millions": self.two_months_ago_level_millions,
            "previous_as_known_value": self.previous_as_known_value,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "previous_artifact_uri": self.previous_artifact_uri,
            "revision_comparable": self.revision_comparable,
            "comparison_basis": self.comparison_basis,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "measure_id": self.measure_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CensusDurableMeasureV1:
        return cls(
            measure_key=str(data.get("measure_key", "")),
            reference_period=str(data.get("reference_period", "")),
            actual_value=cast(float, data.get("actual_value")),
            actual_lexical=str(data.get("actual_lexical", "")),
            revised_previous_value=cast(
                float, data.get("revised_previous_value")
            ),
            revised_previous_lexical=str(
                data.get("revised_previous_lexical", "")
            ),
            current_level_millions=cast(
                int, data.get("current_level_millions")
            ),
            previous_level_millions=cast(
                int, data.get("previous_level_millions")
            ),
            two_months_ago_level_millions=cast(
                int, data.get("two_months_ago_level_millions")
            ),
            previous_as_known_value=cast(
                float | None, data.get("previous_as_known_value")
            ),
            previous_as_known_lexical=(
                None
                if data.get("previous_as_known_lexical") is None
                else str(data.get("previous_as_known_lexical"))
            ),
            previous_artifact_uri=(
                None
                if data.get("previous_artifact_uri") is None
                else str(data.get("previous_artifact_uri"))
            ),
            revision_comparable=cast(bool, data.get("revision_comparable")),
            comparison_basis=str(data.get("comparison_basis", "")),
            measure_id=str(data.get("measure_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusDurablePublicationV1:
    """One scheduled advance occurrence, including an unavailable notice."""

    reference_period: str
    release_date: str
    released_lexical: str
    released_at_ns: int
    reported_zone: str
    zone_consistent: bool
    artifact_uri: str
    content_sha256: str
    content_length: int
    parser_era: str
    ocr_entry_id: str | None
    officially_unavailable: bool
    historical_revision_notice: bool
    shutdown_delayed: bool
    reported_rate_cross_check_count: int
    measures: tuple[CensusDurableMeasureV1, ...]
    publication_id: str = ""
    schema_version: str = DURABLE_PUBLICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != DURABLE_PUBLICATION_SCHEMA_VERSION:
            raise ValueError("unsupported durable-goods publication schema")
        period = _year_month(self.reference_period, "reference_period")
        release_date = _iso_date(self.release_date, "release_date")
        lexical = _required_text(self.released_lexical, "released_lexical")
        match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})T(08:30|10:00):00", lexical)
        if match is None or match.group(1) != release_date:
            raise ValueError("durable-goods release lexical time differs")
        hour, minute = (int(item) for item in match.group(2).split(":"))
        expected_ns = int(
            datetime.combine(
                date.fromisoformat(release_date),
                time(hour, minute),
                ZoneInfo("America/New_York"),
            ).timestamp()
            * 1_000_000_000
        )
        if self.released_at_ns != expected_ns:
            raise ValueError("durable-goods normalized release time differs")
        zone = _required_text(self.reported_zone, "reported_zone")
        if zone not in {"ET", "EST", "EDT"}:
            raise ValueError("durable-goods reported zone differs")
        if not isinstance(self.zone_consistent, bool):
            raise TypeError("zone_consistent must be boolean")
        uri = _https_uri(self.artifact_uri, "artifact_uri")
        sha = _sha256(self.content_sha256, "content_sha256")
        _positive_int(
            self.content_length,
            "content_length",
            MAX_DURABLE_ARTIFACT_BYTES,
        )
        era = _required_text(self.parser_era, "parser_era")
        if era not in {
            "ocr-pdf-table",
            "native-pdf-table",
            "official-unavailable-notice",
        }:
            raise ValueError("durable-goods parser era differs")
        ocr_id = _optional_text(self.ocr_entry_id)
        if (era == "ocr-pdf-table") != (ocr_id is not None):
            raise ValueError("durable-goods OCR identity differs")
        if ocr_id is not None and not ocr_id.startswith(
            "durable-ocr-entry:sha256:"
        ):
            raise ValueError("durable-goods OCR identity is invalid")
        for name in (
            "officially_unavailable",
            "historical_revision_notice",
            "shutdown_delayed",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        measures = tuple(self.measures)
        if self.officially_unavailable:
            if (
                period != DURABLE_OFFICIALLY_UNAVAILABLE_PERIOD
                or era != "official-unavailable-notice"
                or measures
            ):
                raise ValueError("durable-goods unavailability differs")
        else:
            expected_keys = (
                (DURABLE_HEADLINE_ORDERS,)
                if period in DURABLE_CORE_SOURCE_UNAVAILABLE_PERIODS
                else DURABLE_MEASURE_KEYS
            )
            if tuple(item.measure_key for item in measures) != expected_keys:
                raise ValueError("durable-goods measures differ")
        if any(item.reference_period != period for item in measures):
            raise ValueError("durable-goods measure periods differ")
        cross_checks = _count(
            self.reported_rate_cross_check_count,
            "reported_rate_cross_check_count",
            4,
        )
        if cross_checks != 2 * len(measures):
            raise ValueError("durable-goods rate cross-check count differs")
        object.__setattr__(self, "reference_period", period)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "reported_zone", zone)
        object.__setattr__(self, "artifact_uri", uri)
        object.__setattr__(self, "content_sha256", sha)
        object.__setattr__(self, "parser_era", era)
        object.__setattr__(self, "ocr_entry_id", ocr_id)
        object.__setattr__(self, "measures", measures)
        expected = _stable_id("durable-publication", self.identity_payload())
        if self.publication_id and self.publication_id != expected:
            raise ValueError("durable-goods publication identity differs")
        object.__setattr__(self, "publication_id", expected)

    @property
    def by_measure(self) -> Mapping[str, CensusDurableMeasureV1]:
        return MappingProxyType(
            {item.measure_key: item for item in self.measures}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "release_date": self.release_date,
            "released_lexical": self.released_lexical,
            "released_at_ns": self.released_at_ns,
            "reported_zone": self.reported_zone,
            "zone_consistent": self.zone_consistent,
            "artifact_uri": self.artifact_uri,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "parser_era": self.parser_era,
            "ocr_entry_id": self.ocr_entry_id,
            "officially_unavailable": self.officially_unavailable,
            "historical_revision_notice": self.historical_revision_notice,
            "shutdown_delayed": self.shutdown_delayed,
            "reported_rate_cross_check_count": self.reported_rate_cross_check_count,
            "measures": [item.to_dict() for item in self.measures],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "publication_id": self.publication_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CensusDurablePublicationV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            release_date=str(data.get("release_date", "")),
            released_lexical=str(data.get("released_lexical", "")),
            released_at_ns=cast(int, data.get("released_at_ns")),
            reported_zone=str(data.get("reported_zone", "")),
            zone_consistent=cast(bool, data.get("zone_consistent")),
            artifact_uri=str(data.get("artifact_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            parser_era=str(data.get("parser_era", "")),
            ocr_entry_id=(
                None
                if data.get("ocr_entry_id") is None
                else str(data.get("ocr_entry_id"))
            ),
            officially_unavailable=cast(
                bool, data.get("officially_unavailable")
            ),
            historical_revision_notice=cast(
                bool, data.get("historical_revision_notice")
            ),
            shutdown_delayed=cast(bool, data.get("shutdown_delayed")),
            reported_rate_cross_check_count=cast(
                int, data.get("reported_rate_cross_check_count")
            ),
            measures=tuple(
                CensusDurableMeasureV1.from_dict(
                    _mapping(item, "durable-goods measure")
                )
                for item in _sequence(data.get("measures"), "measures")
            ),
            publication_id=str(data.get("publication_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CensusDurableArchiveManifestV1:
    """Compact closure receipt for the complete advance-report corpus."""

    registry_id: str
    profile_id: str
    release_index: CensusDurableReleaseIndexV1
    ocr_corpus_id: str
    predecessor_content_sha256: str
    predecessor_content_length: int
    publications: tuple[CensusDurablePublicationV1, ...]
    raw_artifact_count: int
    total_content_bytes: int
    measure_occurrence_count: int
    comparable_revision_count: int
    officially_unavailable_count: int
    core_source_unavailable_count: int
    corrected_link_count: int
    ocr_publication_count: int
    historical_revision_notice_count: int
    shutdown_delayed_count: int
    source_zone_error_count: int
    nonstandard_release_time_count: int
    reported_rate_cross_check_count: int
    exact_minute_count: int
    manifest_id: str = ""
    schema_version: str = DURABLE_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != DURABLE_ARCHIVE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported durable-goods archive schema")
        registry = _required_text(self.registry_id, "registry_id")
        profile = _required_text(self.profile_id, "profile_id")
        if not registry.startswith("official-source-registry:sha256:"):
            raise ValueError("durable-goods registry identity is invalid")
        if not profile.startswith("us-backfill-profile:sha256:"):
            raise ValueError("durable-goods profile identity is invalid")
        if not isinstance(self.release_index, CensusDurableReleaseIndexV1):
            raise TypeError("durable-goods manifest requires a v1 index")
        ocr_id = _required_text(self.ocr_corpus_id, "ocr_corpus_id")
        if not ocr_id.startswith("durable-ocr-corpus:sha256:"):
            raise ValueError("durable-goods OCR identity is invalid")
        predecessor_sha = _sha256(
            self.predecessor_content_sha256, "predecessor_content_sha256"
        )
        _positive_int(
            self.predecessor_content_length,
            "predecessor_content_length",
            MAX_DURABLE_ARTIFACT_BYTES,
        )
        publications = tuple(self.publications)
        if tuple(item.reference_period for item in publications) != tuple(
            item.reference_period for item in self.release_index.releases
        ):
            raise ValueError("durable-goods manifest differs from index")
        dates = tuple(item.release_date for item in publications)
        if dates != tuple(sorted(dates)) or len(set(dates)) != len(dates):
            raise ValueError("durable-goods release dates are not increasing")
        expected_counts = {
            "raw_artifact_count": 2 + len(publications),
            "measure_occurrence_count": sum(
                len(item.measures) for item in publications
            ),
            "comparable_revision_count": sum(
                measure.previous_was_revised
                for item in publications
                for measure in item.measures
            ),
            "officially_unavailable_count": sum(
                item.officially_unavailable for item in publications
            ),
            "core_source_unavailable_count": sum(
                not item.officially_unavailable
                and DURABLE_CORE_CAPITAL_GOODS not in item.by_measure
                for item in publications
            ),
            "corrected_link_count": sum(
                item.link_corrected for item in self.release_index.releases
            ),
            "ocr_publication_count": sum(
                item.ocr_entry_id is not None for item in publications
            ),
            "historical_revision_notice_count": sum(
                item.historical_revision_notice for item in publications
            ),
            "shutdown_delayed_count": sum(
                item.shutdown_delayed for item in publications
            ),
            "source_zone_error_count": sum(
                not item.zone_consistent for item in publications
            ),
            "nonstandard_release_time_count": sum(
                "T10:00:00" in item.released_lexical for item in publications
            ),
            "reported_rate_cross_check_count": sum(
                item.reported_rate_cross_check_count for item in publications
            ),
            "exact_minute_count": len(publications),
        }
        for name, expected in expected_counts.items():
            if _count(getattr(self, name), name) != expected:
                raise ValueError(f"durable-goods {name} differs")
        expected_bytes = (
            self.release_index.content_length
            + self.predecessor_content_length
            + sum(item.content_length for item in publications)
        )
        if self.total_content_bytes != expected_bytes:
            raise ValueError("durable-goods total source bytes differ")
        object.__setattr__(self, "registry_id", registry)
        object.__setattr__(self, "profile_id", profile)
        object.__setattr__(self, "ocr_corpus_id", ocr_id)
        object.__setattr__(self, "predecessor_content_sha256", predecessor_sha)
        object.__setattr__(self, "publications", publications)
        expected_id = _stable_id(
            "durable-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected_id:
            raise ValueError("durable-goods manifest identity differs")
        object.__setattr__(self, "manifest_id", expected_id)

    @property
    def by_period(self) -> Mapping[str, CensusDurablePublicationV1]:
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
            "predecessor_content_sha256": self.predecessor_content_sha256,
            "predecessor_content_length": self.predecessor_content_length,
            "publications": [item.to_dict() for item in self.publications],
            "raw_artifact_count": self.raw_artifact_count,
            "total_content_bytes": self.total_content_bytes,
            "measure_occurrence_count": self.measure_occurrence_count,
            "comparable_revision_count": self.comparable_revision_count,
            "officially_unavailable_count": self.officially_unavailable_count,
            "core_source_unavailable_count": self.core_source_unavailable_count,
            "corrected_link_count": self.corrected_link_count,
            "ocr_publication_count": self.ocr_publication_count,
            "historical_revision_notice_count": self.historical_revision_notice_count,
            "shutdown_delayed_count": self.shutdown_delayed_count,
            "source_zone_error_count": self.source_zone_error_count,
            "nonstandard_release_time_count": self.nonstandard_release_time_count,
            "reported_rate_cross_check_count": self.reported_rate_cross_check_count,
            "exact_minute_count": self.exact_minute_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CensusDurableArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=CensusDurableReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "durable-goods index")
            ),
            ocr_corpus_id=str(data.get("ocr_corpus_id", "")),
            predecessor_content_sha256=str(
                data.get("predecessor_content_sha256", "")
            ),
            predecessor_content_length=cast(
                int, data.get("predecessor_content_length")
            ),
            publications=tuple(
                CensusDurablePublicationV1.from_dict(
                    _mapping(item, "durable-goods publication")
                )
                for item in _sequence(data.get("publications"), "publications")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            measure_occurrence_count=cast(
                int, data.get("measure_occurrence_count")
            ),
            comparable_revision_count=cast(
                int, data.get("comparable_revision_count")
            ),
            officially_unavailable_count=cast(
                int, data.get("officially_unavailable_count")
            ),
            core_source_unavailable_count=cast(
                int, data.get("core_source_unavailable_count")
            ),
            corrected_link_count=cast(int, data.get("corrected_link_count")),
            ocr_publication_count=cast(int, data.get("ocr_publication_count")),
            historical_revision_notice_count=cast(
                int, data.get("historical_revision_notice_count")
            ),
            shutdown_delayed_count=cast(
                int, data.get("shutdown_delayed_count")
            ),
            source_zone_error_count=cast(
                int, data.get("source_zone_error_count")
            ),
            nonstandard_release_time_count=cast(
                int, data.get("nonstandard_release_time_count")
            ),
            reported_rate_cross_check_count=cast(
                int, data.get("reported_rate_cross_check_count")
            ),
            exact_minute_count=cast(int, data.get("exact_minute_count")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> CensusDurableArchiveManifestV1:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("durable-goods manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(payload, "durable-goods manifest"))


def build_census_durable_index_request(
    registry: OfficialSourceRegistryV1, *, as_of_date: str
) -> OfficialSourceRequestV1:
    """Plan the exact official Advance Durable Goods archive request."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("durable-goods index request requires a v1 registry")
    as_of = _iso_date(as_of_date, "as_of_date")
    source = registry.source(DURABLE_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=DURABLE_INDEX_URI,
        source_format=OfficialSourceFormat.HTML,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=US_BACKFILL_START_DATE,
        window_end=as_of,
        page_number=1,
    )


def parse_census_durable_release_index(
    snapshot: OfficialRawSnapshotV1, *, as_of_date: str
) -> CensusDurableReleaseIndexV1:
    """Parse and require the gap-free 2000-present advance PDF inventory."""
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("durable-goods index parser requires a v1 snapshot")
    as_of = _iso_date(as_of_date, "as_of_date")
    if (
        snapshot.request.source_key != DURABLE_SOURCE_KEY
        or snapshot.request.uri != DURABLE_INDEX_URI
        or snapshot.request.source_format is not OfficialSourceFormat.HTML
        or snapshot.status_code != 200
        or len(snapshot.content) > MAX_DURABLE_INDEX_BYTES
    ):
        raise ValueError("durable-goods index snapshot differs")
    try:
        text = snapshot.content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("durable-goods index is not UTF-8") from exc
    observed: dict[str, str] = {}
    for href in _HREF_RE.finditer(text):
        uri = urljoin(DURABLE_INDEX_URI, html.unescape(href.group("uri")))
        match = _ARCHIVE_LINK_RE.search(urlparse(uri).path)
        if match is None:
            continue
        period = _period_from_link(match)
        if period < DURABLE_FIRST_REFERENCE_PERIOD:
            continue
        prior = observed.setdefault(period, uri)
        if prior != uri:
            raise ValueError("durable-goods index repeats a period")
    if not observed:
        raise ValueError("durable-goods index contains no target releases")
    releases = tuple(
        CensusDurableReleaseIndexEntryV1(
            reference_period=period,
            listed_uri=uri,
            artifact_uri=(
                DURABLE_JULY_2005_ARTIFACT_URI
                if uri == DURABLE_JULY_2005_LISTED_URI
                else uri
            ),
            link_corrected=uri == DURABLE_JULY_2005_LISTED_URI,
        )
        for period, uri in sorted(observed.items())
    )
    return CensusDurableReleaseIndexV1(
        source_uri=DURABLE_INDEX_URI,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        as_of_date=as_of,
        releases=releases,
    )


def _release_request(
    registry: OfficialSourceRegistryV1, *, uri: str, period: str
) -> OfficialSourceRequestV1:
    source = registry.source(DURABLE_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=OfficialSourceFormat.PDF,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=f"{period}-01",
        window_end=f"{period}-{calendar.monthrange(*map(int, period.split('-')))[1]:02d}",
    )


def build_census_durable_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: CensusDurableReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Plan the predecessor and every indexed advance-report PDF."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("durable-goods requests require a v1 registry")
    if not isinstance(release_index, CensusDurableReleaseIndexV1):
        raise TypeError("durable-goods requests require a v1 index")
    requests = [
        _release_request(
            registry,
            uri=DURABLE_PREDECESSOR_URI,
            period=DURABLE_PREDECESSOR_REFERENCE_PERIOD,
        ),
        *(
            _release_request(
                registry,
                uri=item.artifact_uri,
                period=item.reference_period,
            )
            for item in release_index.releases
        ),
    ]
    if len({item.uri for item in requests}) != len(requests):
        raise ValueError("durable-goods request plan repeats a URI")
    return tuple(requests)


def _validate_pdf_snapshot(
    snapshot: OfficialRawSnapshotV1, *, uri: str
) -> None:
    if (
        not isinstance(snapshot, OfficialRawSnapshotV1)
        or snapshot.request.source_key != DURABLE_SOURCE_KEY
        or snapshot.request.uri != uri
        or snapshot.request.source_format is not OfficialSourceFormat.PDF
        or snapshot.status_code != 200
        or not snapshot.content.startswith(b"%PDF-")
        or len(snapshot.content) > MAX_DURABLE_ARTIFACT_BYTES
    ):
        raise ValueError("durable-goods PDF snapshot differs")


def _pdf_pages(content: bytes) -> tuple[str, ...]:
    try:
        return tuple(
            page.extract_text() or ""
            for page in PdfReader(BytesIO(content)).pages
        )
    except Exception as exc:
        raise ValueError(
            "durable-goods artifact is not a readable PDF"
        ) from exc


def _normalize_table_text(text: str) -> str:
    result = text.translate(_HYPHEN_TRANSLATION).replace("\xa0", " ")
    result = re.sub(r"(?<=\d)\s*,\s*(?=\d{3}\b)", ",", result)
    result = re.sub(r"(?<=\d)\.(?=\d{3}\b)", ",", result)
    return re.sub(r"(?<=\d)\.\s+(?=\d\b)", ".", result)


def _row_after(lines: Sequence[str], start: int) -> tuple[str, ...]:
    for index in range(start, min(start + 15, len(lines))):
        match = re.search(
            r"(?:New|w)\s*Orders\s*[24]?", lines[index], re.IGNORECASE
        )
        if match is None:
            continue
        combined = " ".join(lines[index : index + 3])
        match = re.search(
            r"(?:New|w)\s*Orders\s*[24]?", combined, re.IGNORECASE
        )
        assert match is not None
        values = _NUMBER_RE.findall(
            _normalize_table_text(combined[match.end() :])
        )
        if len(values) >= 6:
            return tuple(values[:6])
    raise ValueError("durable-goods table row is missing")


def _parse_int_token(value: str) -> int:
    normalized = value.replace(",", "")
    if not normalized.isdigit():
        raise ValueError(f"durable-goods level is not an integer: {value!r}")
    return int(normalized)


def _reported_rate(
    value: str, current: int, previous: int
) -> tuple[str, float]:
    magnitude = Decimal(
        value.replace(",", "").replace("+", "").replace("-", "")
    )
    signed = (
        -magnitude
        if current < previous
        else magnitude if current > previous else Decimal(0)
    )
    return _canonical_percentage(signed)


def _computed_rate(current: int, previous: int) -> Decimal:
    return (
        (Decimal(current) / Decimal(previous) - Decimal(1)) * Decimal(100)
    ).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)


def _parse_measure_row(
    values: tuple[str, ...], *, content_sha256: str, measure_key: str
) -> tuple[tuple[int, int, int], tuple[tuple[str, float], tuple[str, float]]]:
    override = _OCR_ROW_OVERRIDES.get((content_sha256, measure_key))
    if override is not None:
        levels, rate_texts = override
        rates = (
            _percentage(rate_texts[0], "actual rate override"),
            _percentage(rate_texts[1], "previous rate override"),
        )
    else:
        levels = (
            _parse_int_token(values[0]),
            _parse_int_token(values[1]),
            _parse_int_token(values[2]),
        )
        rates = (
            _reported_rate(values[3], levels[0], levels[1]),
            _reported_rate(values[4], levels[1], levels[2]),
        )
    for index, (current, previous) in enumerate(
        ((levels[0], levels[1]), (levels[1], levels[2]))
    ):
        if abs(
            Decimal(rates[index][0]) - _computed_rate(current, previous)
        ) > Decimal("0.1"):
            raise ValueError(
                "durable-goods reported rate fails its level cross-check"
            )
    return levels, rates


@dataclass(frozen=True, slots=True)
class _ParsedRelease:
    release_date: str
    hour: int
    minute: int
    reported_zone: str
    zone_consistent: bool
    parser_era: str
    ocr_entry_id: str | None
    officially_unavailable: bool
    historical_revision_notice: bool
    rows: Mapping[
        str,
        tuple[
            tuple[int, int, int], tuple[tuple[str, float], tuple[str, float]]
        ],
    ]


def _release_metadata(
    text: str, content_sha256: str
) -> tuple[str, int, int, str, bool]:
    override = _RELEASE_METADATA_OVERRIDES.get(content_sha256)
    if override is not None:
        release_date, hour, minute, zone = override
    else:
        header = " ".join(text[:3500].replace("\xa0", " ").split())
        time_match = _TIME_RE.search(header)
        date_matches = tuple(_DATE_RE.finditer(header))
        if time_match is None or not date_matches:
            raise ValueError("durable-goods release header is incomplete")
        date_match = min(
            date_matches,
            key=lambda item: min(
                abs(item.start() - time_match.start()),
                abs(item.end() - time_match.start()),
            ),
        )
        parsed_date = date(
            int(date_match.group("year")),
            _FULL_MONTH_NUMBER[date_match.group("month").lower()],
            int(date_match.group("day")),
        )
        release_date = parsed_date.isoformat()
        hour = int(time_match.group("hour"))
        minute = int(time_match.group("minute"))
        zone = time_match.group("zone").upper()
    expected_zone = datetime.combine(
        date.fromisoformat(release_date),
        time(hour, minute),
        ZoneInfo("America/New_York"),
    ).tzname()
    consistent = zone == "ET" or zone == expected_zone
    return release_date, hour, minute, zone, consistent


def _historical_revision_notice(text: str) -> bool:
    normalized = " ".join(text.replace("\xa0", " ").split())
    return bool(
        re.search(
            r"revised (?:and )?(?:from )?recently benchmarked|"
            r"(?:benchmark )?notice revised historical data.{0,160}"
            r"(?:will be|were) issued|"
            r"revised historical series.{0,160}released|"
            r"notice of revisions(?: and changes)? due to updated seasonal|"
            r"historic benchmark revisions included in this report",
            normalized,
            re.IGNORECASE,
        )
    )


def _parse_release(
    *,
    reference_period: str,
    snapshot: OfficialRawSnapshotV1,
    ocr_corpus: CensusDurableOcrCorpusV1,
) -> _ParsedRelease:
    _validate_pdf_snapshot(snapshot, uri=snapshot.request.uri)
    native_pages = _pdf_pages(snapshot.content)
    ocr_entry = ocr_corpus.by_uri.get(snapshot.request.uri)
    if ocr_entry is not None:
        if ocr_entry.content_sha256 != snapshot.content_sha256:
            raise ValueError("durable-goods OCR source hash differs")
        pages = ocr_entry.page_texts
        parser_era = "ocr-pdf-table"
        ocr_entry_id: str | None = ocr_entry.entry_id
    else:
        pages = native_pages
        parser_era = "native-pdf-table"
        ocr_entry_id = None
    text = "\f".join(pages)
    release_date, hour, minute, zone, consistent = _release_metadata(
        text, snapshot.content_sha256
    )
    unavailable = reference_period == DURABLE_OFFICIALLY_UNAVAILABLE_PERIOD
    if unavailable:
        if "unable to recreate the November Advance Report" not in text:
            raise ValueError("durable-goods unavailable notice differs")
        return _ParsedRelease(
            release_date=release_date,
            hour=hour,
            minute=minute,
            reported_zone=zone,
            zone_consistent=consistent,
            parser_era="official-unavailable-notice",
            ocr_entry_id=None,
            officially_unavailable=True,
            historical_revision_notice=False,
            rows=MappingProxyType({}),
        )
    table = next(
        (
            page
            for page in pages
            if re.search(
                r"Table\s*1[.:]?\s*.{0,40}DURABLE\s+GOODS\s+MANUFACTURERS",
                page.replace("\xa0", " "),
                re.IGNORECASE | re.DOTALL,
            )
        ),
        None,
    )
    if table is None:
        raise ValueError(
            f"durable-goods Table 1 is missing for {reference_period}"
        )
    lines = _normalize_table_text(table).splitlines()
    starts = [
        index
        for index, line in enumerate(lines)
        if re.fullmatch(r"\s*DURABLE\s+GOODS\s*", line, re.IGNORECASE)
    ]
    start = starts[-1] if starts else 5
    headline_values = _row_after(lines, start)
    rows = {
        DURABLE_HEADLINE_ORDERS: _parse_measure_row(
            headline_values,
            content_sha256=snapshot.content_sha256,
            measure_key=DURABLE_HEADLINE_ORDERS,
        )
    }
    core_heading = next(
        (
            index
            for index, line in enumerate(lines[start:], start)
            if re.search(
                r"Exclud(?:ing|ina)\s+aircraft(?:\s+and\s+parts)?\s*:",
                line,
                re.IGNORECASE,
            )
        ),
        None,
    )
    if core_heading is None:
        if reference_period not in DURABLE_CORE_SOURCE_UNAVAILABLE_PERIODS:
            raise ValueError("durable-goods core table row is missing")
    else:
        if reference_period in DURABLE_CORE_SOURCE_UNAVAILABLE_PERIODS:
            raise ValueError("durable-goods unexpected core table row")
        rows[DURABLE_CORE_CAPITAL_GOODS] = _parse_measure_row(
            _row_after(lines, core_heading),
            content_sha256=snapshot.content_sha256,
            measure_key=DURABLE_CORE_CAPITAL_GOODS,
        )
    return _ParsedRelease(
        release_date=release_date,
        hour=hour,
        minute=minute,
        reported_zone=zone,
        zone_consistent=consistent,
        parser_era=parser_era,
        ocr_entry_id=ocr_entry_id,
        officially_unavailable=False,
        historical_revision_notice=_historical_revision_notice(text),
        rows=MappingProxyType(rows),
    )


def build_census_durable_archive_manifest(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    release_index: CensusDurableReleaseIndexV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    ocr_corpus: CensusDurableOcrCorpusV1,
) -> CensusDurableArchiveManifestV1:
    """Reconstruct all occurrences and their previous-as-known lineage."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("durable-goods manifest requires a v1 registry")
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("durable-goods manifest requires a v1 profile")
    source = registry.source(DURABLE_SOURCE_KEY)
    program = profile.by_key[DURABLE_PROGRAM_KEY]
    if (
        profile.registry_id != registry.registry_id
        or program.source_key != source.source_key
        or program.archive_uri != DURABLE_INDEX_URI
    ):
        raise ValueError("durable-goods registry and profile differ")
    requests = build_census_durable_archive_requests(registry, release_index)
    if set(snapshots_by_uri) != {item.uri for item in requests}:
        raise ValueError("durable-goods retained artifact set differs")
    for request in requests:
        _validate_pdf_snapshot(snapshots_by_uri[request.uri], uri=request.uri)
    predecessor_snapshot = snapshots_by_uri[DURABLE_PREDECESSOR_URI]
    predecessor = _parse_release(
        reference_period=DURABLE_PREDECESSOR_REFERENCE_PERIOD,
        snapshot=predecessor_snapshot,
        ocr_corpus=ocr_corpus,
    )
    known: dict[str, tuple[str, float, str]] = {}
    for key, (_, rates) in predecessor.rows.items():
        known[key] = (rates[0][0], rates[0][1], DURABLE_PREDECESSOR_URI)
    publications: list[CensusDurablePublicationV1] = []
    used_ocr_uris = {DURABLE_PREDECESSOR_URI}
    for entry in release_index.releases:
        snapshot = snapshots_by_uri[entry.artifact_uri]
        try:
            parsed = _parse_release(
                reference_period=entry.reference_period,
                snapshot=snapshot,
                ocr_corpus=ocr_corpus,
            )
        except ValueError as exc:
            raise ValueError(
                f"durable-goods release parse failed for {entry.reference_period}"
            ) from exc
        if parsed.ocr_entry_id is not None:
            used_ocr_uris.add(entry.artifact_uri)
        measures: list[CensusDurableMeasureV1] = []
        for key in DURABLE_MEASURE_KEYS:
            row = parsed.rows.get(key)
            if row is None:
                continue
            levels, rates = row
            if (
                key == DURABLE_CORE_CAPITAL_GOODS
                and entry.reference_period
                == _next_month(DURABLE_CORE_SOURCE_UNAVAILABLE_PERIODS[-1])
            ):
                previous_text: str | None = None
                previous_value: float | None = None
                previous_uri: str | None = None
                comparable = False
                basis = "prior-core-measure-unavailable"
            elif entry.reference_period == "2009-12":
                previous_text, previous_value = rates[1]
                previous_uri = entry.artifact_uri
                comparable = False
                basis = "intervening-historical-correction"
            else:
                previous_text, previous_value, previous_uri = known[key]
                comparable = True
                basis = "previous-advance-release"
            measures.append(
                CensusDurableMeasureV1(
                    measure_key=key,
                    reference_period=entry.reference_period,
                    actual_value=rates[0][1],
                    actual_lexical=rates[0][0],
                    revised_previous_value=rates[1][1],
                    revised_previous_lexical=rates[1][0],
                    current_level_millions=levels[0],
                    previous_level_millions=levels[1],
                    two_months_ago_level_millions=levels[2],
                    previous_as_known_value=previous_value,
                    previous_as_known_lexical=previous_text,
                    previous_artifact_uri=previous_uri,
                    revision_comparable=comparable,
                    comparison_basis=basis,
                )
            )
            known[key] = (rates[0][0], rates[0][1], entry.artifact_uri)
        released_lexical = (
            f"{parsed.release_date}T{parsed.hour:02d}:{parsed.minute:02d}:00"
        )
        released_at_ns = int(
            datetime.combine(
                date.fromisoformat(parsed.release_date),
                time(parsed.hour, parsed.minute),
                ZoneInfo("America/New_York"),
            ).timestamp()
            * 1_000_000_000
        )
        publications.append(
            CensusDurablePublicationV1(
                reference_period=entry.reference_period,
                release_date=parsed.release_date,
                released_lexical=released_lexical,
                released_at_ns=released_at_ns,
                reported_zone=parsed.reported_zone,
                zone_consistent=parsed.zone_consistent,
                artifact_uri=entry.artifact_uri,
                content_sha256=snapshot.content_sha256,
                content_length=len(snapshot.content),
                parser_era=parsed.parser_era,
                ocr_entry_id=parsed.ocr_entry_id,
                officially_unavailable=parsed.officially_unavailable,
                historical_revision_notice=parsed.historical_revision_notice,
                shutdown_delayed=(
                    entry.reference_period in DURABLE_SHUTDOWN_DELAYED_PERIODS
                ),
                reported_rate_cross_check_count=2 * len(measures),
                measures=tuple(measures),
            )
        )
    if used_ocr_uris != set(ocr_corpus.by_uri):
        raise ValueError("durable-goods OCR corpus has unused evidence")
    publication_tuple = tuple(publications)
    return CensusDurableArchiveManifestV1(
        registry_id=registry.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        ocr_corpus_id=ocr_corpus.corpus_id,
        predecessor_content_sha256=predecessor_snapshot.content_sha256,
        predecessor_content_length=len(predecessor_snapshot.content),
        publications=publication_tuple,
        raw_artifact_count=2 + len(publication_tuple),
        total_content_bytes=(
            release_index.content_length
            + len(predecessor_snapshot.content)
            + sum(item.content_length for item in publication_tuple)
        ),
        measure_occurrence_count=sum(
            len(item.measures) for item in publication_tuple
        ),
        comparable_revision_count=sum(
            measure.previous_was_revised
            for item in publication_tuple
            for measure in item.measures
        ),
        officially_unavailable_count=sum(
            item.officially_unavailable for item in publication_tuple
        ),
        core_source_unavailable_count=sum(
            not item.officially_unavailable
            and DURABLE_CORE_CAPITAL_GOODS not in item.by_measure
            for item in publication_tuple
        ),
        corrected_link_count=sum(
            item.link_corrected for item in release_index.releases
        ),
        ocr_publication_count=sum(
            item.ocr_entry_id is not None for item in publication_tuple
        ),
        historical_revision_notice_count=sum(
            item.historical_revision_notice for item in publication_tuple
        ),
        shutdown_delayed_count=sum(
            item.shutdown_delayed for item in publication_tuple
        ),
        source_zone_error_count=sum(
            not item.zone_consistent for item in publication_tuple
        ),
        nonstandard_release_time_count=sum(
            "T10:00:00" in item.released_lexical for item in publication_tuple
        ),
        reported_rate_cross_check_count=sum(
            item.reported_rate_cross_check_count for item in publication_tuple
        ),
        exact_minute_count=len(publication_tuple),
    )


def replay_census_durable_archive(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    index_snapshot: OfficialRawSnapshotV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    ocr_corpus: CensusDurableOcrCorpusV1,
    expected: CensusDurableArchiveManifestV1,
) -> CensusDurableArchiveManifestV1:
    """Recompute and compare the complete retained M3 advance corpus."""
    if not isinstance(expected, CensusDurableArchiveManifestV1):
        raise TypeError("durable-goods replay requires a v1 expected manifest")
    release_index = parse_census_durable_release_index(
        index_snapshot, as_of_date=expected.release_index.as_of_date
    )
    rebuilt = build_census_durable_archive_manifest(
        registry, profile, release_index, snapshots_by_uri, ocr_corpus
    )
    if rebuilt != expected:
        raise ValueError("durable-goods retained-corpus replay differs")
    return rebuilt


def census_durable_coverage_from_manifest(
    manifest: CensusDurableArchiveManifestV1,
) -> UnitedStatesProgramCoverageV1:
    """Derive complete U.S. durable-goods coverage from the manifest."""
    if not isinstance(manifest, CensusDurableArchiveManifestV1):
        raise TypeError("durable-goods coverage requires a v1 manifest")
    available = (
        len(manifest.publications) - manifest.officially_unavailable_count
    )
    return UnitedStatesProgramCoverageV1(
        program_key=DURABLE_PROGRAM_KEY,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=manifest.release_index.as_of_date,
        expected_occurrence_count=len(manifest.publications),
        schedule_count=len(manifest.publications),
        initial_actual_count=available,
        previous_as_known_count=available,
        revision_count=manifest.comparable_revision_count,
        exact_minute_count=manifest.exact_minute_count,
        forecast_count=0,
        artifact_sha256s=tuple(
            sorted(
                {
                    manifest.release_index.content_sha256,
                    manifest.predecessor_content_sha256,
                    *(item.content_sha256 for item in manifest.publications),
                }
            )
        ),
        gap_reasons=(
            UnitedStatesCoverageGapReason.OFFICIAL_MEASURE_UNAVAILABLE,
        ),
        notes=(
            "The official November 2009 notice says the advance report could not be recreated; no value is manufactured.",
            "The source omits the core capital-goods row from April 2001 through April 2002 while headline occurrence coverage remains complete.",
            "Three source-authored 10:00 releases and ten incorrect DST abbreviations remain explicit.",
            "No historical event-level consensus is manufactured from another survey.",
        ),
        officially_unavailable_count=manifest.officially_unavailable_count,
    )


def packaged_census_durable_archive_manifest_path() -> Path:
    """Return the packaged durable-goods manifest path."""
    return (
        Path(__file__).with_name("assets") / "us_durable_goods_archive_v1.json"
    )


def packaged_census_durable_index_path() -> Path:
    """Return the packaged exact archive-index envelope path."""
    return Path(__file__).with_name("assets") / "us_durable_goods_index_v1.json"


def packaged_census_durable_ocr_path() -> Path:
    """Return the packaged reviewed OCR corpus path."""
    return Path(__file__).with_name("assets") / "us_durable_goods_ocr_v1.json"


def load_packaged_census_durable_archive_manifest() -> (
    CensusDurableArchiveManifestV1
):
    """Load and validate the packaged durable-goods closure receipt."""
    return CensusDurableArchiveManifestV1.from_json(
        packaged_census_durable_archive_manifest_path().read_text(
            encoding="utf-8"
        )
    )


def load_packaged_census_durable_ocr_corpus() -> CensusDurableOcrCorpusV1:
    """Load the packaged hash-bound reviewed OCR corpus."""
    return CensusDurableOcrCorpusV1.from_json(
        packaged_census_durable_ocr_path().read_text(encoding="utf-8")
    )


def load_packaged_census_durable_index_snapshot() -> OfficialRawSnapshotV1:
    """Load the exact official archive page from its compact envelope."""
    try:
        payload = _mapping(
            json.loads(
                packaged_census_durable_index_path().read_text(encoding="utf-8")
            ),
            "durable-goods index envelope",
        )
    except json.JSONDecodeError as exc:
        raise ValueError(
            "durable-goods index envelope is invalid JSON"
        ) from exc
    if payload.get("schema_version") != DURABLE_INDEX_ENVELOPE_SCHEMA_VERSION:
        raise ValueError("unsupported durable-goods index-envelope schema")
    as_of = _iso_date(payload.get("as_of_date"), "as_of_date")
    captured_at_ns = cast(int, payload.get("captured_at_ns"))
    _positive_int(captured_at_ns, "captured_at_ns", 2**63 - 1)
    uri = str(payload.get("uri", ""))
    if uri != DURABLE_INDEX_URI:
        raise ValueError("durable-goods index-envelope URI differs")
    try:
        content = base64.b64decode(
            str(payload.get("content_base64", "")), validate=True
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "durable-goods index envelope has invalid base64"
        ) from exc
    registry = load_packaged_official_source_registry()
    request = build_census_durable_index_request(registry, as_of_date=as_of)
    snapshot = OfficialRawSnapshotV1(
        request=request,
        retrieved_at_ns=captured_at_ns,
        completed_at_ns=captured_at_ns,
        status_code=200,
        resolved_uri=uri,
        response_headers={"Content-Type": "text/html"},
        content=content,
        content_type="text/html",
    )
    if snapshot.content_sha256 != _sha256(
        payload.get("content_sha256"), "content_sha256"
    ):
        raise ValueError("durable-goods index-envelope content differs")
    return snapshot


def load_packaged_census_durable_index() -> CensusDurableReleaseIndexV1:
    """Replay the packaged exact archive page into its typed index."""
    snapshot = load_packaged_census_durable_index_snapshot()
    assert snapshot.request.window_end is not None
    return parse_census_durable_release_index(
        snapshot, as_of_date=snapshot.request.window_end
    )


__all__ = [
    "DURABLE_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "DURABLE_CORE_CAPITAL_GOODS",
    "DURABLE_CORE_SOURCE_UNAVAILABLE_PERIODS",
    "DURABLE_FIRST_REFERENCE_PERIOD",
    "DURABLE_HEADLINE_ORDERS",
    "DURABLE_INDEX_URI",
    "DURABLE_JULY_2005_ARTIFACT_URI",
    "DURABLE_JULY_2005_LISTED_URI",
    "DURABLE_MEASURE_KEYS",
    "DURABLE_OFFICIALLY_UNAVAILABLE_PERIOD",
    "DURABLE_PREDECESSOR_REFERENCE_PERIOD",
    "DURABLE_PREDECESSOR_URI",
    "DURABLE_PROGRAM_KEY",
    "DURABLE_RELEASE_SCHEDULE_URI",
    "DURABLE_SHUTDOWN_DELAYED_PERIODS",
    "DURABLE_SOURCE_KEY",
    "CensusDurableArchiveManifestV1",
    "CensusDurableMeasureV1",
    "CensusDurableOcrCorpusV1",
    "CensusDurableOcrEntryV1",
    "CensusDurablePublicationV1",
    "CensusDurableReleaseIndexEntryV1",
    "CensusDurableReleaseIndexV1",
    "build_census_durable_archive_manifest",
    "build_census_durable_archive_requests",
    "build_census_durable_index_request",
    "census_durable_coverage_from_manifest",
    "load_packaged_census_durable_archive_manifest",
    "load_packaged_census_durable_index",
    "load_packaged_census_durable_index_snapshot",
    "load_packaged_census_durable_ocr_corpus",
    "packaged_census_durable_archive_manifest_path",
    "packaged_census_durable_index_path",
    "packaged_census_durable_ocr_path",
    "parse_census_durable_release_index",
    "replay_census_durable_archive",
]
