"""Complete BEA Gross Domestic Product archive qualification for issue #538.

The archive keeps advance, second, and third/final quarterly estimates as
separate release occurrences.  Every occurrence binds the real-GDP headline
to the immediately preceding estimate and, for advance estimates, to the
prior-quarter value printed in the new release.  This preserves annual and
comprehensive update effects instead of rewriting earlier vintages.
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
from io import BytesIO
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

from pypdf import PdfReader

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.economic_calendar import EconomicReleaseStage
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
    UnitedStatesProgramCoverageV1,
)
from histdatacom.runtime_contracts import JSONValue

BEA_GDP_INDEX_PAGE_SCHEMA_VERSION = "histdatacom.bea-gdp-index-page.v1"
BEA_GDP_INDEX_ENTRY_SCHEMA_VERSION = "histdatacom.bea-gdp-index-entry.v1"
BEA_GDP_INDEX_SCHEMA_VERSION = "histdatacom.bea-gdp-index.v1"
BEA_GDP_OBSERVATION_SCHEMA_VERSION = "histdatacom.bea-gdp-observation.v1"
BEA_GDP_ARCHIVE_ENTRY_SCHEMA_VERSION = "histdatacom.bea-gdp-archive-entry.v1"
BEA_GDP_ARCHIVE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.bea-gdp-archive-manifest.v1"
)
BEA_GDP_INDEX_ENVELOPE_SCHEMA_VERSION = "histdatacom.bea-gdp-index-envelope.v1"

BEA_GDP_SOURCE_KEY = "us.bea.gdp"
BEA_GDP_PROGRAM_KEY = "us.bea.gdp"
BEA_GDP_INDEX_URI = (
    "https://www.bea.gov/news/archive?created_1=All&"
    "field_related_product_target_id=451&title="
)
BEA_GDP_SUPPLEMENTAL_URI = (
    "https://www.bea.gov/news/2026/"
    "gdp-second-estimate-and-corporate-profits-2nd-quarter-2026"
)
BEA_GDP_SUPPLEMENTAL_DATE = "2026-08-26"
BEA_GDP_SUPPLEMENTAL_TITLE = (
    "GDP (Second Estimate) and Corporate Profits, 2nd Quarter 2026"
)
BEA_GDP_PREDECESSOR_URI = (
    "https://www.bea.gov/sites/default/files/2019-04/gdp399f.pdf"
)
BEA_GDP_PREDECESSOR_DATE = "1999-12-22"
BEA_GDP_TIME_SUPPORT_URI = (
    "https://www.bea.gov/sites/default/files/newsreleases/national/gdp/"
    "2001/pdf/gdp400f.pdf"
)
BEA_GDP_TIME_SUPPORT_DATE = "2001-03-29"
BEA_GDP_ARCHIVE_PAGE_COUNT = 18
BEA_GDP_MEASURE_KEY = "real-gross-domestic-product"
BEA_GDP_ARCHIVE_DATE_CORRECTIONS = MappingProxyType(
    {
        "https://www.bea.gov/news/2009/gross-domestic-product-third-quarter-2009-advance-estimate": "2009-10-29",
        "https://www.bea.gov/news/2018/gross-domestic-product-3rd-quarter-2018-advance-estimate": "2018-10-26",
    }
)

MAX_BEA_GDP_INDEX_PAGES = 64
MAX_BEA_GDP_RELEASES = 512
MAX_BEA_GDP_INDEX_PAGE_BYTES = 4 * 1024 * 1024
MAX_BEA_GDP_RELEASE_BYTES = 32 * 1024 * 1024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_QUARTER_RE = re.compile(r"^(?P<year>\d{4})-Q(?P<quarter>[1-4])$")
_TITLE_PERIOD_RE = re.compile(
    r"\b(?:(?P<number>[1-4])(?:st|nd|rd|th)|"
    r"(?P<word>first|second|third|fourth))\s+quarter"
    r"(?:\s+and\s+(?:annual|year))?\s+(?P<year>\d{4})",
    re.IGNORECASE,
)
_ACTUAL_RE = re.compile(
    r"real gross domestic product.{0,420}?\b"
    r"(?P<direction>increased|decreased)\s+at an annual rate of\s+"
    r"(?P<value>[0-9]+(?:\.[0-9]+)?)\s+percent",
    re.IGNORECASE,
)
_ACTUAL_PERIOD_RE = re.compile(
    r"in\s+the\s+"
    r"(?P<quarter>first|second|third|fourth|[1-4](?:st|nd|rd|th))"
    r"[- ]+quarter(?:\s+of)?\s+(?P<year>\d{4})",
    re.IGNORECASE,
)
_PRIOR_QUARTER_RE = re.compile(
    r"\bIn the\s+"
    r"(?P<quarter>first|second|third|fourth|[1-4](?:st|nd|rd|th))"
    r"[- ]+quarter(?:\s+of\s+(?P<year>\d{4}))?\s*,?\s*"
    r"real GDP\s+(?:also\s+)?"
    r"(?P<direction>increased|decreased)\s+"
    r"(?P<value>[0-9]+(?:\.[0-9]+)?)\s+percent",
    re.IGNORECASE,
)
_RELEASE_TIME_RE = re.compile(
    r"8:30\s*(?:A\.?M\.?|a\.?m\.?)\s*,?\s*"
    r"(?P<zone>EST|EDT)\s*,\s*"
    r"(?:(?:Monday|Tuesday|Wednesday|Thursday|Friday)\s*,\s*)?"
    r"(?P<date>[A-Za-z]+\s+\d{1,2}\s*,\s*\d{4})",
    re.IGNORECASE,
)
_LONG_DATE_RE = re.compile(
    r"^(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2})\s*,\s*" r"(?P<year>\d{4})$"
)
_ARCHIVE_ROW_RE = re.compile(r"(?is)<tr\b[^>]*>(?P<body>.*?)</tr>")
_ARCHIVE_TITLE_RE = re.compile(
    r"(?is)<td\b[^>]*class=[\"'][^\"']*views-field-title[^\"']*"
    r"[\"'][^>]*>.*?<a\b[^>]*href=[\"'](?P<uri>[^\"']+)"
    r"[\"'][^>]*>(?P<title>.*?)</a>"
)
_ARCHIVE_DATE_RE = re.compile(
    r"(?is)<td\b[^>]*class=[\"'][^\"']*views-field-created[^\"']*"
    r"[\"'][^>]*>(?P<date>.*?)</td>"
)
_MONTH_NUMBERS = {
    name.lower(): number
    for number, name in enumerate(calendar.month_name)
    if name
}
_QUARTER_NUMBERS = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
}
_TITLE_UPDATE_MARKERS = (
    "annual update",
    "comprehensive update",
    "comprehensive revision",
    "revised estimates",
)


def _required_text(value: object, name: str) -> str:
    result = str(value).strip()
    if not result or len(result) > 8192:
        raise ValueError(f"{name} is invalid")
    return result


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        parsed = date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != result:
        raise ValueError(f"{name} must be a canonical ISO date")
    return result


def _quarter(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _QUARTER_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a canonical quarter")
    return result


def _previous_quarter(value: str) -> str:
    match = _QUARTER_RE.fullmatch(_quarter(value, "quarter"))
    assert match is not None
    year = int(match.group("year"))
    quarter = int(match.group("quarter"))
    if quarter == 1:
        return f"{year - 1:04d}-Q4"
    return f"{year:04d}-Q{quarter - 1}"


def _https_uri(value: object, name: str) -> str:
    result = _required_text(value, name)
    parsed = urlparse(result)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.bea.gov"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError(f"{name} is not an official BEA HTTPS URI")
    return result


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return result


def _positive_int(value: object, name: str, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 1 <= value <= maximum
    ):
        raise ValueError(f"{name} is outside its bound")
    return value


def _nonnegative_int(value: object, name: str, maximum: int) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 <= value <= maximum
    ):
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


def _numeric_lexical(value: object, name: str) -> tuple[str, float]:
    lexical = _required_text(value, name)
    if re.fullmatch(r"-?(?:0|[1-9]\d*)\.\d+", lexical) is None:
        raise ValueError(f"{name} must be a signed decimal lexical value")
    return lexical, _finite(lexical, name)


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be a sequence")
    return value


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = str(canonical_contract_json(payload)).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _visible_html(value: str) -> str:
    result = re.sub(r"(?i)<br\s*/?>", " ", value)
    result = re.sub(r"(?is)<[^>]+>", " ", result)
    return " ".join(html.unescape(result).replace("\xa0", " ").split())


def _decode_html(content: bytes) -> str:
    for encoding in ("utf-8-sig", "windows-1252"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("BEA GDP HTML is neither UTF-8 nor Windows-1252")


def _pdf_text(content: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(content))
        if not 1 <= len(reader.pages) <= 128:
            raise ValueError("BEA GDP PDF page count is outside its bound")
        result = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as exc:
        raise ValueError("BEA GDP PDF cannot be parsed") from exc
    if not result.strip():
        raise ValueError("BEA GDP PDF has no extractable text")
    return result


def _signed_lexical(direction: str, magnitude: str) -> tuple[str, float]:
    value = float(magnitude)
    if direction.lower() == "decreased":
        value = -value
    lexical = f"{value:.1f}"
    return lexical, value


def _quarter_number(value: str) -> int:
    lowered = value.lower()
    if lowered in _QUARTER_NUMBERS:
        return _QUARTER_NUMBERS[lowered]
    return int(lowered[0])


def _period_from_title(title: str) -> str:
    match = _TITLE_PERIOD_RE.search(title)
    if match is None:
        raise ValueError("BEA GDP title omits its reference quarter")
    quarter = (
        int(match.group("number"))
        if match.group("number") is not None
        else _QUARTER_NUMBERS[match.group("word").lower()]
    )
    return f"{int(match.group('year')):04d}-Q{quarter}"


def _long_date(value: str) -> date:
    match = _LONG_DATE_RE.fullmatch(" ".join(value.split()))
    if match is None or match.group("month").lower() not in _MONTH_NUMBERS:
        raise ValueError("BEA GDP long date format changed")
    try:
        return date(
            int(match.group("year")),
            _MONTH_NUMBERS[match.group("month").lower()],
            int(match.group("day")),
        )
    except ValueError as exc:
        raise ValueError("BEA GDP long date is invalid") from exc


def _stage_from_title(title: str) -> EconomicReleaseStage:
    lowered = title.lower()
    if (
        "advance" in lowered
        or "(initial estimate)" in lowered
        or lowered.startswith("initial gross")
    ):
        return EconomicReleaseStage.ADVANCE
    if (
        "third estimate" in lowered
        or '"final"' in lowered
        or "(final)" in lowered
        or "updated estimate" in lowered
    ):
        return EconomicReleaseStage.FINAL
    if (
        "second estimate" in lowered
        or '"preliminary"' in lowered
        or "(preliminary)" in lowered
    ):
        return EconomicReleaseStage.SECOND
    raise ValueError("BEA GDP title omits a recognized release stage")


def _archive_page_uri(page_number: int) -> str:
    _nonnegative_int(page_number, "page_number", MAX_BEA_GDP_INDEX_PAGES - 1)
    if page_number == 0:
        return BEA_GDP_INDEX_URI
    return f"{BEA_GDP_INDEX_URI}&page={page_number}"


def _shutdown_disruption_count(
    releases: Sequence[BeaGdpReleaseIndexEntryV1],
) -> int:
    by_period: dict[str, set[EconomicReleaseStage]] = {}
    for item in releases:
        by_period.setdefault(item.reference_period, set()).add(
            item.release_stage
        )
    return sum(
        EconomicReleaseStage.ADVANCE in stages
        and EconomicReleaseStage.FINAL in stages
        and EconomicReleaseStage.SECOND not in stages
        for stages in by_period.values()
    )


@dataclass(frozen=True, slots=True)
class BeaGdpReleaseIndexPageV1:
    """One exact page of the official BEA GDP archive inventory."""

    page_number: int
    source_uri: str
    content_sha256: str
    content_length: int
    schema_version: str = BEA_GDP_INDEX_PAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BEA_GDP_INDEX_PAGE_SCHEMA_VERSION:
            raise ValueError("unsupported BEA GDP index-page schema")
        page = _nonnegative_int(
            self.page_number, "page_number", MAX_BEA_GDP_INDEX_PAGES - 1
        )
        source = _https_uri(self.source_uri, "source_uri")
        if source != _archive_page_uri(page):
            raise ValueError("BEA GDP index-page URI differs from page number")
        object.__setattr__(self, "page_number", page)
        object.__setattr__(self, "source_uri", source)
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        _positive_int(
            self.content_length,
            "content_length",
            MAX_BEA_GDP_INDEX_PAGE_BYTES,
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "page_number": self.page_number,
            "source_uri": self.source_uri,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BeaGdpReleaseIndexPageV1:
        return cls(
            page_number=cast(int, data.get("page_number")),
            source_uri=str(data.get("source_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BeaGdpReleaseIndexEntryV1:
    """One quarterly estimate occurrence selected from the BEA inventory."""

    reference_period: str
    release_stage: EconomicReleaseStage
    release_date: str
    archive_published_date: str
    artifact_uri: str
    title: str
    archive_listed: bool
    entry_id: str = ""
    schema_version: str = BEA_GDP_INDEX_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BEA_GDP_INDEX_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported BEA GDP index-entry schema")
        reference = _quarter(self.reference_period, "reference_period")
        stage = EconomicReleaseStage.from_value(self.release_stage)
        if stage not in {
            EconomicReleaseStage.ADVANCE,
            EconomicReleaseStage.SECOND,
            EconomicReleaseStage.FINAL,
        }:
            raise ValueError("BEA GDP index stage is invalid")
        release = _iso_date(self.release_date, "release_date")
        archive_published = _iso_date(
            self.archive_published_date, "archive_published_date"
        )
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        title = _required_text(self.title, "title")
        if reference != _period_from_title(
            title
        ) or stage is not _stage_from_title(title):
            raise ValueError("BEA GDP index identity differs from its title")
        if not isinstance(self.archive_listed, bool):
            raise TypeError("archive_listed must be boolean")
        if release != BEA_GDP_ARCHIVE_DATE_CORRECTIONS.get(
            artifact, archive_published
        ):
            raise ValueError(
                "BEA GDP release and archive dates differ unexpectedly"
            )
        if not self.archive_listed and (
            artifact != BEA_GDP_SUPPLEMENTAL_URI
            or release != BEA_GDP_SUPPLEMENTAL_DATE
            or title != BEA_GDP_SUPPLEMENTAL_TITLE
        ):
            raise ValueError("unexpected supplemental BEA GDP release")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "release_stage", stage)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "archive_published_date", archive_published)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "title", title)
        expected = _stable_id("bea-gdp-index-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("BEA GDP index-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    @property
    def stage_key(self) -> str:
        return f"{self.reference_period}:{self.release_stage.value}"

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "release_stage": self.release_stage.value,
            "release_date": self.release_date,
            "archive_published_date": self.archive_published_date,
            "artifact_uri": self.artifact_uri,
            "title": self.title,
            "archive_listed": self.archive_listed,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BeaGdpReleaseIndexEntryV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            release_stage=EconomicReleaseStage.from_value(
                str(data.get("release_stage", ""))
            ),
            release_date=str(data.get("release_date", "")),
            archive_published_date=str(data.get("archive_published_date", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            title=str(data.get("title", "")),
            archive_listed=cast(bool, data.get("archive_listed")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BeaGdpReleaseIndexV1:
    """Content-addressed multi-page BEA GDP release inventory."""

    pages: tuple[BeaGdpReleaseIndexPageV1, ...]
    as_of_date: str
    releases: tuple[BeaGdpReleaseIndexEntryV1, ...]
    index_id: str = ""
    schema_version: str = BEA_GDP_INDEX_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BEA_GDP_INDEX_SCHEMA_VERSION:
            raise ValueError("unsupported BEA GDP index schema")
        pages = tuple(self.pages)
        if (
            not pages
            or len(pages) > MAX_BEA_GDP_INDEX_PAGES
            or any(
                not isinstance(item, BeaGdpReleaseIndexPageV1) for item in pages
            )
            or tuple(item.page_number for item in pages)
            != tuple(range(len(pages)))
        ):
            raise ValueError("BEA GDP index pages must be contiguous")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        releases = tuple(self.releases)
        if (
            not releases
            or len(releases) > MAX_BEA_GDP_RELEASES
            or any(
                not isinstance(item, BeaGdpReleaseIndexEntryV1)
                for item in releases
            )
            or releases
            != tuple(sorted(releases, key=lambda item: item.release_date))
            or len({item.artifact_uri for item in releases}) != len(releases)
            or len({item.stage_key for item in releases}) != len(releases)
            or any(item.release_date > as_of for item in releases)
            or any(
                item.release_date < US_BACKFILL_START_DATE for item in releases
            )
        ):
            raise ValueError("BEA GDP release inventory is invalid")
        by_period: dict[str, list[EconomicReleaseStage]] = {}
        for item in releases:
            by_period.setdefault(item.reference_period, []).append(
                item.release_stage
            )
        order = {
            EconomicReleaseStage.ADVANCE: 0,
            EconomicReleaseStage.SECOND: 1,
            EconomicReleaseStage.FINAL: 2,
        }
        if any(
            stages != sorted(set(stages), key=order.__getitem__)
            or stages[0] is not EconomicReleaseStage.ADVANCE
            for stages in by_period.values()
        ):
            raise ValueError("BEA GDP stages are duplicated or out of order")
        object.__setattr__(self, "pages", pages)
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "releases", releases)
        expected = _stable_id("bea-gdp-index", self.identity_payload())
        if self.index_id and self.index_id != expected:
            raise ValueError("BEA GDP index identity differs")
        object.__setattr__(self, "index_id", expected)

    @property
    def by_stage_key(self) -> Mapping[str, BeaGdpReleaseIndexEntryV1]:
        return MappingProxyType(
            {item.stage_key: item for item in self.releases}
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "pages": [item.to_dict() for item in self.pages],
            "as_of_date": self.as_of_date,
            "releases": [item.to_dict() for item in self.releases],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "index_id": self.index_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BeaGdpReleaseIndexV1:
        return cls(
            pages=tuple(
                BeaGdpReleaseIndexPageV1.from_dict(
                    _mapping(item, "BEA GDP index page")
                )
                for item in _sequence(data.get("pages"), "pages")
            ),
            as_of_date=str(data.get("as_of_date", "")),
            releases=tuple(
                BeaGdpReleaseIndexEntryV1.from_dict(
                    _mapping(item, "BEA GDP index entry")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            index_id=str(data.get("index_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BeaGdpReleaseObservationV1:
    """Headline real-GDP observation parsed from one release artifact."""

    reference_period: str
    release_stage: EconomicReleaseStage
    release_date: str
    artifact_uri: str
    content_sha256: str
    content_length: int
    released_lexical: str
    release_header_lexical: str
    reported_zone: str
    zone_consistent: bool
    actual_value: float
    actual_lexical: str
    prior_period_value: float | None
    prior_period_lexical: str | None
    prior_period_reference: str | None
    source_era: str
    release_time_locator: str
    actual_locator: str
    prior_period_locator: str | None
    historical_update_notice: bool
    correction_notice: bool
    shutdown_replacement: bool
    release_time_support_uri: str | None = None
    release_time_support_sha256: str | None = None
    release_time_support_length: int | None = None
    observation_id: str = ""
    schema_version: str = BEA_GDP_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BEA_GDP_OBSERVATION_SCHEMA_VERSION:
            raise ValueError("unsupported BEA GDP observation schema")
        reference = _quarter(self.reference_period, "reference_period")
        stage = EconomicReleaseStage.from_value(self.release_stage)
        release = _iso_date(self.release_date, "release_date")
        artifact = _https_uri(self.artifact_uri, "artifact_uri")
        digest = _sha256(self.content_sha256, "content_sha256")
        _positive_int(
            self.content_length, "content_length", MAX_BEA_GDP_RELEASE_BYTES
        )
        released = _required_text(self.released_lexical, "released_lexical")
        if released != f"{release}T08:30:00":
            raise ValueError("BEA GDP normalized release time differs")
        header = _required_text(
            self.release_header_lexical, "release_header_lexical"
        )
        zone = _required_text(self.reported_zone, "reported_zone").upper()
        if zone not in {"EST", "EDT"}:
            raise ValueError("BEA GDP release zone is invalid")
        if not isinstance(self.zone_consistent, bool):
            raise TypeError("zone_consistent must be boolean")
        actual_lexical, actual = _numeric_lexical(
            self.actual_lexical, "actual_lexical"
        )
        if not math.isclose(
            actual, _finite(self.actual_value, "actual_value"), abs_tol=1e-15
        ):
            raise ValueError("BEA GDP actual lexical and numeric values differ")
        prior_reference = self.prior_period_reference
        prior_lexical = self.prior_period_lexical
        prior_value = self.prior_period_value
        prior_locator = self.prior_period_locator
        if stage is EconomicReleaseStage.ADVANCE:
            if (
                prior_reference is None
                or prior_lexical is None
                or prior_value is None
            ):
                raise ValueError(
                    "BEA GDP advance observation lacks prior quarter"
                )
            prior_reference = _quarter(
                prior_reference, "prior_period_reference"
            )
            if prior_reference != _previous_quarter(reference):
                raise ValueError("BEA GDP advance prior quarter differs")
            prior_lexical, parsed_prior = _numeric_lexical(
                prior_lexical, "prior_period_lexical"
            )
            if not math.isclose(
                parsed_prior,
                _finite(prior_value, "prior_period_value"),
                abs_tol=1e-15,
            ):
                raise ValueError(
                    "BEA GDP prior lexical and numeric values differ"
                )
            if prior_locator is None:
                raise ValueError(
                    "BEA GDP advance prior quarter lacks a locator"
                )
            prior_locator = _required_text(
                prior_locator, "prior_period_locator"
            )
        elif any(
            item is not None
            for item in (
                prior_reference,
                prior_lexical,
                prior_value,
                prior_locator,
            )
        ):
            raise ValueError("only an advance release reports a prior quarter")
        era = _required_text(self.source_era, "source_era")
        if era not in {
            "fixed-width-preformatted-html",
            "narrative-html",
            "comparison-table-html",
        }:
            raise ValueError("BEA GDP source era is invalid")
        for name in (
            "historical_update_notice",
            "correction_notice",
            "shutdown_replacement",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TypeError(f"{name} must be boolean")
        support_uri = self.release_time_support_uri
        support_sha = self.release_time_support_sha256
        support_length = self.release_time_support_length
        if release == BEA_GDP_TIME_SUPPORT_DATE:
            if support_uri != BEA_GDP_TIME_SUPPORT_URI:
                raise ValueError("BEA GDP release-time support URI differs")
            support_uri = _https_uri(support_uri, "release_time_support_uri")
            support_sha = _sha256(support_sha, "release_time_support_sha256")
            _positive_int(
                support_length,
                "release_time_support_length",
                MAX_BEA_GDP_RELEASE_BYTES,
            )
        elif any(
            item is not None
            for item in (support_uri, support_sha, support_length)
        ):
            raise ValueError("unexpected BEA GDP release-time support artifact")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "release_stage", stage)
        object.__setattr__(self, "release_date", release)
        object.__setattr__(self, "artifact_uri", artifact)
        object.__setattr__(self, "content_sha256", digest)
        object.__setattr__(self, "released_lexical", released)
        object.__setattr__(self, "release_header_lexical", header)
        object.__setattr__(self, "reported_zone", zone)
        object.__setattr__(self, "actual_value", actual)
        object.__setattr__(self, "actual_lexical", actual_lexical)
        object.__setattr__(self, "prior_period_reference", prior_reference)
        object.__setattr__(self, "prior_period_lexical", prior_lexical)
        object.__setattr__(self, "prior_period_value", prior_value)
        object.__setattr__(self, "source_era", era)
        object.__setattr__(
            self,
            "release_time_locator",
            _required_text(self.release_time_locator, "release_time_locator"),
        )
        object.__setattr__(
            self,
            "actual_locator",
            _required_text(self.actual_locator, "actual_locator"),
        )
        object.__setattr__(self, "prior_period_locator", prior_locator)
        object.__setattr__(self, "release_time_support_uri", support_uri)
        object.__setattr__(self, "release_time_support_sha256", support_sha)
        expected = _stable_id("bea-gdp-observation", self.identity_payload())
        if self.observation_id and self.observation_id != expected:
            raise ValueError("BEA GDP observation identity differs")
        object.__setattr__(self, "observation_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "release_stage": self.release_stage.value,
            "release_date": self.release_date,
            "artifact_uri": self.artifact_uri,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "released_lexical": self.released_lexical,
            "release_header_lexical": self.release_header_lexical,
            "reported_zone": self.reported_zone,
            "zone_consistent": self.zone_consistent,
            "actual_value": self.actual_value,
            "actual_lexical": self.actual_lexical,
            "prior_period_value": self.prior_period_value,
            "prior_period_lexical": self.prior_period_lexical,
            "prior_period_reference": self.prior_period_reference,
            "source_era": self.source_era,
            "release_time_locator": self.release_time_locator,
            "actual_locator": self.actual_locator,
            "prior_period_locator": self.prior_period_locator,
            "historical_update_notice": self.historical_update_notice,
            "correction_notice": self.correction_notice,
            "shutdown_replacement": self.shutdown_replacement,
            "release_time_support_uri": self.release_time_support_uri,
            "release_time_support_sha256": self.release_time_support_sha256,
            "release_time_support_length": self.release_time_support_length,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "observation_id": self.observation_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BeaGdpReleaseObservationV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            release_stage=EconomicReleaseStage.from_value(
                str(data.get("release_stage", ""))
            ),
            release_date=str(data.get("release_date", "")),
            artifact_uri=str(data.get("artifact_uri", "")),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            released_lexical=str(data.get("released_lexical", "")),
            release_header_lexical=str(data.get("release_header_lexical", "")),
            reported_zone=str(data.get("reported_zone", "")),
            zone_consistent=cast(bool, data.get("zone_consistent")),
            actual_value=cast(float, data.get("actual_value")),
            actual_lexical=str(data.get("actual_lexical", "")),
            prior_period_value=cast(
                float | None, data.get("prior_period_value")
            ),
            prior_period_lexical=cast(
                str | None, data.get("prior_period_lexical")
            ),
            prior_period_reference=cast(
                str | None, data.get("prior_period_reference")
            ),
            source_era=str(data.get("source_era", "")),
            release_time_locator=str(data.get("release_time_locator", "")),
            actual_locator=str(data.get("actual_locator", "")),
            prior_period_locator=cast(
                str | None, data.get("prior_period_locator")
            ),
            historical_update_notice=cast(
                bool, data.get("historical_update_notice")
            ),
            correction_notice=cast(bool, data.get("correction_notice")),
            shutdown_replacement=cast(bool, data.get("shutdown_replacement")),
            release_time_support_uri=cast(
                str | None, data.get("release_time_support_uri")
            ),
            release_time_support_sha256=cast(
                str | None, data.get("release_time_support_sha256")
            ),
            release_time_support_length=cast(
                int | None, data.get("release_time_support_length")
            ),
            observation_id=str(data.get("observation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BeaGdpArchiveEntryV1:
    """One occurrence-specific GDP estimate and its revision lineage."""

    observation: BeaGdpReleaseObservationV1
    comparison_reference_period: str
    comparison_kind: str
    previous_artifact_uri: str
    previous_content_sha256: str
    previous_content_length: int
    previous_as_known_value: float
    previous_as_known_lexical: str
    revised_previous_value: float
    revised_previous_lexical: str
    normalized_sha256: str
    entry_id: str = ""
    schema_version: str = BEA_GDP_ARCHIVE_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BEA_GDP_ARCHIVE_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported BEA GDP archive-entry schema")
        if not isinstance(self.observation, BeaGdpReleaseObservationV1):
            raise TypeError("BEA GDP archive entry requires an observation")
        stage = self.observation.release_stage
        comparison = _quarter(
            self.comparison_reference_period, "comparison_reference_period"
        )
        expected_comparison = (
            _previous_quarter(self.observation.reference_period)
            if stage is EconomicReleaseStage.ADVANCE
            else self.observation.reference_period
        )
        if comparison != expected_comparison:
            raise ValueError("BEA GDP comparison period differs from stage")
        kind = _required_text(self.comparison_kind, "comparison_kind")
        expected_kind = (
            "prior-quarter"
            if stage is EconomicReleaseStage.ADVANCE
            else "prior-stage"
        )
        if kind != expected_kind:
            raise ValueError("BEA GDP comparison kind differs from stage")
        previous_uri = _https_uri(
            self.previous_artifact_uri, "previous_artifact_uri"
        )
        previous_sha = _sha256(
            self.previous_content_sha256, "previous_content_sha256"
        )
        _positive_int(
            self.previous_content_length,
            "previous_content_length",
            MAX_BEA_GDP_RELEASE_BYTES,
        )
        previous_lexical, previous = _numeric_lexical(
            self.previous_as_known_lexical, "previous_as_known_lexical"
        )
        revised_lexical, revised = _numeric_lexical(
            self.revised_previous_lexical, "revised_previous_lexical"
        )
        if not math.isclose(
            previous,
            _finite(self.previous_as_known_value, "previous_as_known_value"),
            abs_tol=1e-15,
        ) or not math.isclose(
            revised,
            _finite(self.revised_previous_value, "revised_previous_value"),
            abs_tol=1e-15,
        ):
            raise ValueError("BEA GDP comparison lexical values differ")
        if stage is not EconomicReleaseStage.ADVANCE and not math.isclose(
            revised, self.observation.actual_value, abs_tol=1e-15
        ):
            raise ValueError("BEA GDP stage revision differs from its actual")
        if stage is EconomicReleaseStage.ADVANCE and not math.isclose(
            revised,
            cast(float, self.observation.prior_period_value),
            abs_tol=1e-15,
        ):
            raise ValueError("BEA GDP prior-quarter revision differs")
        normalized = _sha256(self.normalized_sha256, "normalized_sha256")
        expected_normalized = hashlib.sha256(
            str(
                canonical_contract_json(
                    {
                        "measure_key": BEA_GDP_MEASURE_KEY,
                        "reference_period": self.observation.reference_period,
                        "comparison_reference_period": comparison,
                        "release_stage": stage.value,
                        "released_lexical": self.observation.released_lexical,
                        "actual_lexical": self.observation.actual_lexical,
                        "previous_as_known_lexical": previous_lexical,
                        "revised_previous_lexical": revised_lexical,
                    }
                )
            ).encode("utf-8")
        ).hexdigest()
        if normalized != expected_normalized:
            raise ValueError("BEA GDP normalized digest differs")
        object.__setattr__(self, "comparison_reference_period", comparison)
        object.__setattr__(self, "comparison_kind", kind)
        object.__setattr__(self, "previous_artifact_uri", previous_uri)
        object.__setattr__(self, "previous_content_sha256", previous_sha)
        object.__setattr__(self, "previous_as_known_value", previous)
        object.__setattr__(self, "previous_as_known_lexical", previous_lexical)
        object.__setattr__(self, "revised_previous_value", revised)
        object.__setattr__(self, "revised_previous_lexical", revised_lexical)
        object.__setattr__(self, "normalized_sha256", normalized)
        expected = _stable_id("bea-gdp-archive-entry", self.identity_payload())
        if self.entry_id and self.entry_id != expected:
            raise ValueError("BEA GDP archive-entry identity differs")
        object.__setattr__(self, "entry_id", expected)

    @property
    def reference_period(self) -> str:
        return self.observation.reference_period

    @property
    def release_stage(self) -> EconomicReleaseStage:
        return self.observation.release_stage

    @property
    def stage_key(self) -> str:
        return f"{self.reference_period}:{self.release_stage.value}"

    @property
    def value_was_revised(self) -> bool:
        return not math.isclose(
            self.previous_as_known_value,
            self.revised_previous_value,
            abs_tol=1e-15,
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "observation": self.observation.to_dict(),
            "comparison_reference_period": self.comparison_reference_period,
            "comparison_kind": self.comparison_kind,
            "previous_artifact_uri": self.previous_artifact_uri,
            "previous_content_sha256": self.previous_content_sha256,
            "previous_content_length": self.previous_content_length,
            "previous_as_known_value": self.previous_as_known_value,
            "previous_as_known_lexical": self.previous_as_known_lexical,
            "revised_previous_value": self.revised_previous_value,
            "revised_previous_lexical": self.revised_previous_lexical,
            "normalized_sha256": self.normalized_sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BeaGdpArchiveEntryV1:
        return cls(
            observation=BeaGdpReleaseObservationV1.from_dict(
                _mapping(data.get("observation"), "BEA GDP observation")
            ),
            comparison_reference_period=str(
                data.get("comparison_reference_period", "")
            ),
            comparison_kind=str(data.get("comparison_kind", "")),
            previous_artifact_uri=str(data.get("previous_artifact_uri", "")),
            previous_content_sha256=str(
                data.get("previous_content_sha256", "")
            ),
            previous_content_length=cast(
                int, data.get("previous_content_length")
            ),
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
            normalized_sha256=str(data.get("normalized_sha256", "")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BeaGdpArchiveManifestV1:
    """Compact deterministic receipt for the complete GDP release corpus."""

    registry_id: str
    profile_id: str
    release_index: BeaGdpReleaseIndexV1
    predecessor_uri: str
    predecessor_content_sha256: str
    predecessor_content_length: int
    predecessor_actual_value: float
    predecessor_actual_lexical: str
    entries: tuple[BeaGdpArchiveEntryV1, ...]
    raw_artifact_count: int
    total_content_bytes: int
    advance_release_count: int
    second_release_count: int
    final_release_count: int
    nonzero_revision_count: int
    advance_revision_count: int
    second_revision_count: int
    final_revision_count: int
    historical_update_notice_count: int
    correction_notice_count: int
    shutdown_replacement_count: int
    shutdown_disruption_quarter_count: int
    fixed_width_release_count: int
    narrative_release_count: int
    comparison_table_release_count: int
    zone_mismatch_count: int
    archive_date_mismatch_count: int
    manifest_id: str = ""
    schema_version: str = BEA_GDP_ARCHIVE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BEA_GDP_ARCHIVE_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported BEA GDP manifest schema")
        registry = _required_text(self.registry_id, "registry_id")
        profile = _required_text(self.profile_id, "profile_id")
        if not isinstance(self.release_index, BeaGdpReleaseIndexV1):
            raise TypeError("BEA GDP manifest requires a release index")
        predecessor_uri = _https_uri(self.predecessor_uri, "predecessor_uri")
        if predecessor_uri != BEA_GDP_PREDECESSOR_URI:
            raise ValueError("BEA GDP predecessor URI differs")
        predecessor_sha = _sha256(
            self.predecessor_content_sha256, "predecessor_content_sha256"
        )
        _positive_int(
            self.predecessor_content_length,
            "predecessor_content_length",
            MAX_BEA_GDP_RELEASE_BYTES,
        )
        predecessor_lexical, predecessor_value = _numeric_lexical(
            self.predecessor_actual_lexical, "predecessor_actual_lexical"
        )
        if not math.isclose(
            predecessor_value,
            _finite(self.predecessor_actual_value, "predecessor_actual_value"),
            abs_tol=1e-15,
        ):
            raise ValueError("BEA GDP predecessor values differ")
        entries = tuple(self.entries)
        if (
            len(entries) != len(self.release_index.releases)
            or any(
                not isinstance(item, BeaGdpArchiveEntryV1) for item in entries
            )
            or tuple(item.stage_key for item in entries)
            != tuple(item.stage_key for item in self.release_index.releases)
        ):
            raise ValueError("BEA GDP manifest entries differ from its index")
        support = {
            item.observation.release_time_support_uri: cast(
                int, item.observation.release_time_support_length
            )
            for item in entries
            if item.observation.release_time_support_uri is not None
        }
        expected_raw_count = len(entries) + 1 + len(support)
        expected_bytes = (
            sum(item.observation.content_length for item in entries)
            + self.predecessor_content_length
            + sum(support.values())
        )
        expected_counts = {
            "raw_artifact_count": expected_raw_count,
            "total_content_bytes": expected_bytes,
            "advance_release_count": sum(
                item.release_stage is EconomicReleaseStage.ADVANCE
                for item in entries
            ),
            "second_release_count": sum(
                item.release_stage is EconomicReleaseStage.SECOND
                for item in entries
            ),
            "final_release_count": sum(
                item.release_stage is EconomicReleaseStage.FINAL
                for item in entries
            ),
            "nonzero_revision_count": sum(
                item.value_was_revised for item in entries
            ),
            "advance_revision_count": sum(
                item.value_was_revised
                and item.release_stage is EconomicReleaseStage.ADVANCE
                for item in entries
            ),
            "second_revision_count": sum(
                item.value_was_revised
                and item.release_stage is EconomicReleaseStage.SECOND
                for item in entries
            ),
            "final_revision_count": sum(
                item.value_was_revised
                and item.release_stage is EconomicReleaseStage.FINAL
                for item in entries
            ),
            "historical_update_notice_count": sum(
                item.observation.historical_update_notice for item in entries
            ),
            "correction_notice_count": sum(
                item.observation.correction_notice for item in entries
            ),
            "shutdown_replacement_count": sum(
                item.observation.shutdown_replacement for item in entries
            ),
            "shutdown_disruption_quarter_count": _shutdown_disruption_count(
                self.release_index.releases
            ),
            "fixed_width_release_count": sum(
                item.observation.source_era == "fixed-width-preformatted-html"
                for item in entries
            ),
            "narrative_release_count": sum(
                item.observation.source_era == "narrative-html"
                for item in entries
            ),
            "comparison_table_release_count": sum(
                item.observation.source_era == "comparison-table-html"
                for item in entries
            ),
            "zone_mismatch_count": sum(
                not item.observation.zone_consistent for item in entries
            ),
            "archive_date_mismatch_count": sum(
                item.release_date != item.archive_published_date
                for item in self.release_index.releases
            ),
        }
        for name, expected in expected_counts.items():
            supplied = _positive_int(
                getattr(self, name), name, MAX_BEA_GDP_RELEASE_BYTES * 2
            )
            if supplied != expected:
                raise ValueError(f"BEA GDP {name} differs from entries")
        object.__setattr__(self, "registry_id", registry)
        object.__setattr__(self, "profile_id", profile)
        object.__setattr__(self, "predecessor_uri", predecessor_uri)
        object.__setattr__(self, "predecessor_content_sha256", predecessor_sha)
        object.__setattr__(self, "predecessor_actual_value", predecessor_value)
        object.__setattr__(
            self, "predecessor_actual_lexical", predecessor_lexical
        )
        object.__setattr__(self, "entries", entries)
        expected_id = _stable_id(
            "bea-gdp-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected_id:
            raise ValueError("BEA GDP manifest identity differs")
        object.__setattr__(self, "manifest_id", expected_id)

    @property
    def by_stage_key(self) -> Mapping[str, BeaGdpArchiveEntryV1]:
        return MappingProxyType({item.stage_key: item for item in self.entries})

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "profile_id": self.profile_id,
            "release_index": self.release_index.to_dict(),
            "predecessor_uri": self.predecessor_uri,
            "predecessor_content_sha256": self.predecessor_content_sha256,
            "predecessor_content_length": self.predecessor_content_length,
            "predecessor_actual_value": self.predecessor_actual_value,
            "predecessor_actual_lexical": self.predecessor_actual_lexical,
            "entries": [item.to_dict() for item in self.entries],
            "raw_artifact_count": self.raw_artifact_count,
            "total_content_bytes": self.total_content_bytes,
            "advance_release_count": self.advance_release_count,
            "second_release_count": self.second_release_count,
            "final_release_count": self.final_release_count,
            "nonzero_revision_count": self.nonzero_revision_count,
            "advance_revision_count": self.advance_revision_count,
            "second_revision_count": self.second_revision_count,
            "final_revision_count": self.final_revision_count,
            "historical_update_notice_count": self.historical_update_notice_count,
            "correction_notice_count": self.correction_notice_count,
            "shutdown_replacement_count": self.shutdown_replacement_count,
            "shutdown_disruption_quarter_count": (
                self.shutdown_disruption_quarter_count
            ),
            "fixed_width_release_count": self.fixed_width_release_count,
            "narrative_release_count": self.narrative_release_count,
            "comparison_table_release_count": self.comparison_table_release_count,
            "zone_mismatch_count": self.zone_mismatch_count,
            "archive_date_mismatch_count": self.archive_date_mismatch_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BeaGdpArchiveManifestV1:
        return cls(
            registry_id=str(data.get("registry_id", "")),
            profile_id=str(data.get("profile_id", "")),
            release_index=BeaGdpReleaseIndexV1.from_dict(
                _mapping(data.get("release_index"), "BEA GDP release index")
            ),
            predecessor_uri=str(data.get("predecessor_uri", "")),
            predecessor_content_sha256=str(
                data.get("predecessor_content_sha256", "")
            ),
            predecessor_content_length=cast(
                int, data.get("predecessor_content_length")
            ),
            predecessor_actual_value=cast(
                float, data.get("predecessor_actual_value")
            ),
            predecessor_actual_lexical=str(
                data.get("predecessor_actual_lexical", "")
            ),
            entries=tuple(
                BeaGdpArchiveEntryV1.from_dict(
                    _mapping(item, "BEA GDP archive entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            advance_release_count=cast(int, data.get("advance_release_count")),
            second_release_count=cast(int, data.get("second_release_count")),
            final_release_count=cast(int, data.get("final_release_count")),
            nonzero_revision_count=cast(
                int, data.get("nonzero_revision_count")
            ),
            advance_revision_count=cast(
                int, data.get("advance_revision_count")
            ),
            second_revision_count=cast(int, data.get("second_revision_count")),
            final_revision_count=cast(int, data.get("final_revision_count")),
            historical_update_notice_count=cast(
                int, data.get("historical_update_notice_count")
            ),
            correction_notice_count=cast(
                int, data.get("correction_notice_count")
            ),
            shutdown_replacement_count=cast(
                int, data.get("shutdown_replacement_count")
            ),
            shutdown_disruption_quarter_count=cast(
                int, data.get("shutdown_disruption_quarter_count")
            ),
            fixed_width_release_count=cast(
                int, data.get("fixed_width_release_count")
            ),
            narrative_release_count=cast(
                int, data.get("narrative_release_count")
            ),
            comparison_table_release_count=cast(
                int, data.get("comparison_table_release_count")
            ),
            zone_mismatch_count=cast(int, data.get("zone_mismatch_count")),
            archive_date_mismatch_count=cast(
                int, data.get("archive_date_mismatch_count")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, payload: str) -> BeaGdpArchiveManifestV1:
        return cls.from_dict(_mapping(json.loads(payload), "BEA GDP manifest"))


def build_bea_gdp_index_requests(
    registry: OfficialSourceRegistryV1,
    *,
    page_count: int = BEA_GDP_ARCHIVE_PAGE_COUNT,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the bounded page requests for the official GDP archive."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("BEA GDP index requests require a v1 registry")
    count = _positive_int(page_count, "page_count", MAX_BEA_GDP_INDEX_PAGES)
    source = registry.source(BEA_GDP_SOURCE_KEY)
    return tuple(
        OfficialSourceRequestV1(
            source_key=source.source_key,
            source_id=source.source_id,
            method=OfficialRequestMethod.GET,
            uri=_archive_page_uri(page),
            source_format=OfficialSourceFormat.HTML,
            parser_id=source.parser_id,
            parser_version=source.parser_version,
            page_number=page,
        )
        for page in range(count)
    )


def _validate_index_snapshots(
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[OfficialRawSnapshotV1, ...]:
    values = tuple(snapshots)
    if (
        not values
        or len(values) > MAX_BEA_GDP_INDEX_PAGES
        or any(not isinstance(item, OfficialRawSnapshotV1) for item in values)
    ):
        raise ValueError("BEA GDP index snapshots are invalid")
    ordered = tuple(
        sorted(values, key=lambda item: cast(int, item.request.page_number))
    )
    if tuple(item.request.page_number for item in ordered) != tuple(
        range(len(ordered))
    ):
        raise ValueError("BEA GDP index snapshots are not contiguous")
    for page, snapshot in enumerate(ordered):
        if (
            snapshot.request.source_key != BEA_GDP_SOURCE_KEY
            or snapshot.request.source_format is not OfficialSourceFormat.HTML
            or snapshot.request.uri != _archive_page_uri(page)
            or not snapshot.content
            or len(snapshot.content) > MAX_BEA_GDP_INDEX_PAGE_BYTES
        ):
            raise ValueError("BEA GDP index snapshot scope differs")
    return ordered


def parse_bea_gdp_release_index(
    snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> BeaGdpReleaseIndexV1:
    """Parse every archive page plus the bounded current-release supplement."""
    ordered = _validate_index_snapshots(snapshots)
    as_of = _iso_date(as_of_date, "as_of_date")
    releases: list[BeaGdpReleaseIndexEntryV1] = []
    pages: list[BeaGdpReleaseIndexPageV1] = []
    for snapshot in ordered:
        page = cast(int, snapshot.request.page_number)
        pages.append(
            BeaGdpReleaseIndexPageV1(
                page_number=page,
                source_uri=snapshot.request.uri,
                content_sha256=snapshot.content_sha256,
                content_length=len(snapshot.content),
            )
        )
        markup = _decode_html(snapshot.content)
        for row in _ARCHIVE_ROW_RE.finditer(markup):
            title_match = _ARCHIVE_TITLE_RE.search(row.group("body"))
            date_match = _ARCHIVE_DATE_RE.search(row.group("body"))
            if title_match is None or date_match is None:
                continue
            title = _visible_html(title_match.group("title"))
            try:
                reference = _period_from_title(title)
                stage = _stage_from_title(title)
            except ValueError:
                continue
            published = _visible_html(date_match.group("date"))
            released = _long_date(published)
            archive_published_date = released.isoformat()
            if not US_BACKFILL_START_DATE <= archive_published_date <= as_of:
                continue
            artifact_uri = urljoin(
                "https://www.bea.gov",
                html.unescape(title_match.group("uri")),
            )
            release_date = BEA_GDP_ARCHIVE_DATE_CORRECTIONS.get(
                artifact_uri, archive_published_date
            )
            releases.append(
                BeaGdpReleaseIndexEntryV1(
                    reference_period=reference,
                    release_stage=stage,
                    release_date=release_date,
                    archive_published_date=archive_published_date,
                    artifact_uri=artifact_uri,
                    title=title,
                    archive_listed=True,
                )
            )
    if as_of >= BEA_GDP_SUPPLEMENTAL_DATE and not any(
        item.artifact_uri == BEA_GDP_SUPPLEMENTAL_URI for item in releases
    ):
        releases.append(
            BeaGdpReleaseIndexEntryV1(
                reference_period="2026-Q2",
                release_stage=EconomicReleaseStage.SECOND,
                release_date=BEA_GDP_SUPPLEMENTAL_DATE,
                archive_published_date=BEA_GDP_SUPPLEMENTAL_DATE,
                artifact_uri=BEA_GDP_SUPPLEMENTAL_URI,
                title=BEA_GDP_SUPPLEMENTAL_TITLE,
                archive_listed=False,
            )
        )
    return BeaGdpReleaseIndexV1(
        pages=tuple(pages),
        as_of_date=as_of,
        releases=tuple(sorted(releases, key=lambda item: item.release_date)),
    )


def _release_request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    release_date: str,
    source_format: OfficialSourceFormat,
) -> OfficialSourceRequestV1:
    source = registry.source(BEA_GDP_SOURCE_KEY)
    if source_format not in source.formats:
        raise ValueError("BEA GDP source does not declare an artifact format")
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        window_start=release_date,
        window_end=release_date,
    )


def build_bea_gdp_archive_requests(
    registry: OfficialSourceRegistryV1,
    release_index: BeaGdpReleaseIndexV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact request for every release and supporting PDF."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("BEA GDP requests require a v1 registry")
    if not isinstance(release_index, BeaGdpReleaseIndexV1):
        raise TypeError("BEA GDP requests require a v1 release index")
    result = [
        _release_request(
            registry,
            item.artifact_uri,
            item.release_date,
            OfficialSourceFormat.HTML,
        )
        for item in release_index.releases
    ]
    result.extend(
        (
            _release_request(
                registry,
                BEA_GDP_PREDECESSOR_URI,
                BEA_GDP_PREDECESSOR_DATE,
                OfficialSourceFormat.PDF,
            ),
            _release_request(
                registry,
                BEA_GDP_TIME_SUPPORT_URI,
                BEA_GDP_TIME_SUPPORT_DATE,
                OfficialSourceFormat.PDF,
            ),
        )
    )
    return tuple(result)


def _validate_release_snapshot(
    snapshot: OfficialRawSnapshotV1,
    *,
    uri: str,
    release_date: str,
    source_format: OfficialSourceFormat,
) -> None:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("BEA GDP parser requires a v1 snapshot")
    if (
        snapshot.request.source_key != BEA_GDP_SOURCE_KEY
        or snapshot.request.uri != uri
        or snapshot.request.source_format is not source_format
        or snapshot.request.window_start != release_date
        or snapshot.request.window_end != release_date
        or not snapshot.content
        or len(snapshot.content) > MAX_BEA_GDP_RELEASE_BYTES
    ):
        raise ValueError("BEA GDP release snapshot scope differs")


def _release_time(
    visible_text: str,
    release_date: str,
    *,
    locator: str,
) -> tuple[str, str, str, bool, str]:
    match = _RELEASE_TIME_RE.search(visible_text)
    if match is None:
        raise ValueError("BEA GDP release omits an exact release time")
    parsed_date = _long_date(match.group("date"))
    if parsed_date.isoformat() != release_date:
        raise ValueError("BEA GDP release header date differs from index")
    zone = match.group("zone").upper()
    aware = datetime.combine(
        parsed_date, time(8, 30), tzinfo=ZoneInfo("America/New_York")
    )
    return (
        f"{release_date}T08:30:00",
        " ".join(match.group(0).split()),
        zone,
        aware.tzname() == zone,
        locator,
    )


def parse_bea_gdp_release(
    snapshot: OfficialRawSnapshotV1,
    index_entry: BeaGdpReleaseIndexEntryV1,
    *,
    release_time_support: OfficialRawSnapshotV1 | None = None,
) -> BeaGdpReleaseObservationV1:
    """Parse one real-GDP headline while preserving source lexical evidence."""
    if not isinstance(index_entry, BeaGdpReleaseIndexEntryV1):
        raise TypeError("BEA GDP parser requires a v1 index entry")
    _validate_release_snapshot(
        snapshot,
        uri=index_entry.artifact_uri,
        release_date=index_entry.release_date,
        source_format=OfficialSourceFormat.HTML,
    )
    markup = _decode_html(snapshot.content)
    visible = _visible_html(markup)
    actual_match = _ACTUAL_RE.search(visible)
    if actual_match is None:
        raise ValueError("BEA GDP release omits the real-GDP headline")
    actual_lexical, actual_value = _signed_lexical(
        actual_match.group("direction"), actual_match.group("value")
    )
    period_match = _ACTUAL_PERIOD_RE.search(
        visible, actual_match.start(), actual_match.end() + 250
    )
    if period_match is not None:
        observed_period = (
            f"{int(period_match.group('year')):04d}-"
            f"Q{_quarter_number(period_match.group('quarter'))}"
        )
        if observed_period != index_entry.reference_period:
            raise ValueError("BEA GDP headline period differs from index")
    support_uri: str | None = None
    support_sha: str | None = None
    support_length: int | None = None
    try:
        released, header, zone, zone_consistent, time_locator = _release_time(
            visible,
            index_entry.release_date,
            locator="html:visible-release-header",
        )
    except ValueError:
        if (
            index_entry.release_date != BEA_GDP_TIME_SUPPORT_DATE
            or release_time_support is None
        ):
            raise
        _validate_release_snapshot(
            release_time_support,
            uri=BEA_GDP_TIME_SUPPORT_URI,
            release_date=BEA_GDP_TIME_SUPPORT_DATE,
            source_format=OfficialSourceFormat.PDF,
        )
        released, header, zone, zone_consistent, time_locator = _release_time(
            " ".join(_pdf_text(release_time_support.content).split()),
            index_entry.release_date,
            locator="supporting-pdf:page-1:release-header",
        )
        support_uri = release_time_support.request.uri
        support_sha = release_time_support.content_sha256
        support_length = len(release_time_support.content)
    prior_reference: str | None = None
    prior_lexical: str | None = None
    prior_value: float | None = None
    prior_locator: str | None = None
    if index_entry.release_stage is EconomicReleaseStage.ADVANCE:
        prior_match = _PRIOR_QUARTER_RE.search(visible, actual_match.end())
        if prior_match is None:
            raise ValueError("BEA GDP advance release omits its prior quarter")
        prior_reference = _previous_quarter(index_entry.reference_period)
        expected_quarter = int(prior_reference[-1])
        if _quarter_number(prior_match.group("quarter")) != expected_quarter:
            raise ValueError("BEA GDP reported prior quarter differs")
        if prior_match.group("year") is not None and int(
            prior_match.group("year")
        ) != int(prior_reference[:4]):
            raise ValueError("BEA GDP reported prior-quarter year differs")
        prior_lexical, prior_value = _signed_lexical(
            prior_match.group("direction"), prior_match.group("value")
        )
        prior_locator = "html:opening-prior-quarter-sentence"
    body_match = re.search(
        r"(?is)<div\b[^>]*class=[\"'][^\"']*field--name-body[^\"']*"
        r"[\"'][^>]*>(?P<body>.*?)(?:</div>\s*</div>|</article>)",
        markup,
    )
    body_markup = body_match.group("body") if body_match is not None else markup
    lowered_markup = markup.lower()
    body_start = lowered_markup.find("field--name-body")
    actual_markup_position = lowered_markup.find(
        "annual rate of", max(0, body_start)
    )
    pre_open = lowered_markup.rfind(
        "<pre", max(0, body_start), actual_markup_position
    )
    pre_close = lowered_markup.rfind(
        "</pre", max(0, body_start), actual_markup_position
    )
    if pre_open > pre_close:
        era = "fixed-width-preformatted-html"
    elif (
        "real gdp and related measures" in visible.lower()
        and "<table" in body_markup.lower()
    ):
        era = "comparison-table-html"
    else:
        era = "narrative-html"
    lowered_title = index_entry.title.lower()
    return BeaGdpReleaseObservationV1(
        reference_period=index_entry.reference_period,
        release_stage=index_entry.release_stage,
        release_date=index_entry.release_date,
        artifact_uri=index_entry.artifact_uri,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        released_lexical=released,
        release_header_lexical=header,
        reported_zone=zone,
        zone_consistent=zone_consistent,
        actual_value=actual_value,
        actual_lexical=actual_lexical,
        prior_period_value=prior_value,
        prior_period_lexical=prior_lexical,
        prior_period_reference=prior_reference,
        source_era=era,
        release_time_locator=time_locator,
        actual_locator="html:opening-real-gdp-headline",
        prior_period_locator=prior_locator,
        historical_update_notice=any(
            marker in lowered_title for marker in _TITLE_UPDATE_MARKERS
        ),
        correction_notice=lowered_title.startswith("corrected "),
        shutdown_replacement=(
            "initial estimate" in lowered_title
            or "updated estimate" in lowered_title
            or lowered_title.startswith("initial gross")
        ),
        release_time_support_uri=support_uri,
        release_time_support_sha256=support_sha,
        release_time_support_length=support_length,
    )


def _normalized_sha256(
    observation: BeaGdpReleaseObservationV1,
    comparison_period: str,
    previous_lexical: str,
    revised_lexical: str,
) -> str:
    payload: dict[str, JSONValue] = {
        "measure_key": BEA_GDP_MEASURE_KEY,
        "reference_period": observation.reference_period,
        "comparison_reference_period": comparison_period,
        "release_stage": observation.release_stage.value,
        "released_lexical": observation.released_lexical,
        "actual_lexical": observation.actual_lexical,
        "previous_as_known_lexical": previous_lexical,
        "revised_previous_lexical": revised_lexical,
    }
    return hashlib.sha256(
        str(canonical_contract_json(payload)).encode("utf-8")
    ).hexdigest()


def build_bea_gdp_archive_manifest(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    release_index: BeaGdpReleaseIndexV1,
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
) -> BeaGdpArchiveManifestV1:
    """Build the complete GDP manifest from an exact retained corpus."""
    if not isinstance(registry, OfficialSourceRegistryV1):
        raise TypeError("BEA GDP manifest requires a v1 registry")
    if not isinstance(profile, UnitedStatesBackfillProfileV1):
        raise TypeError("BEA GDP manifest requires a v1 U.S. profile")
    if not isinstance(release_index, BeaGdpReleaseIndexV1):
        raise TypeError("BEA GDP manifest requires a v1 release index")
    program = profile.by_key[BEA_GDP_PROGRAM_KEY]
    if program.source_key != BEA_GDP_SOURCE_KEY:
        raise ValueError("BEA GDP profile source differs")
    expected_uris = {
        *(item.artifact_uri for item in release_index.releases),
        BEA_GDP_PREDECESSOR_URI,
        BEA_GDP_TIME_SUPPORT_URI,
    }
    if set(snapshots_by_uri) != expected_uris:
        raise ValueError("BEA GDP retained corpus URI set differs")
    predecessor = snapshots_by_uri[BEA_GDP_PREDECESSOR_URI]
    _validate_release_snapshot(
        predecessor,
        uri=BEA_GDP_PREDECESSOR_URI,
        release_date=BEA_GDP_PREDECESSOR_DATE,
        source_format=OfficialSourceFormat.PDF,
    )
    predecessor_text = " ".join(_pdf_text(predecessor.content).split())
    predecessor_match = _ACTUAL_RE.search(predecessor_text)
    if predecessor_match is None:
        raise ValueError("BEA GDP predecessor omits its headline")
    predecessor_lexical, predecessor_value = _signed_lexical(
        predecessor_match.group("direction"), predecessor_match.group("value")
    )
    observations = tuple(
        parse_bea_gdp_release(
            snapshots_by_uri[index_entry.artifact_uri],
            index_entry,
            release_time_support=(
                snapshots_by_uri[BEA_GDP_TIME_SUPPORT_URI]
                if index_entry.release_date == BEA_GDP_TIME_SUPPORT_DATE
                else None
            ),
        )
        for index_entry in release_index.releases
    )
    entries: list[BeaGdpArchiveEntryV1] = []
    last_by_period: dict[str, tuple[float, str, str, str, int]] = {
        "1999-Q3": (
            predecessor_value,
            predecessor_lexical,
            BEA_GDP_PREDECESSOR_URI,
            predecessor.content_sha256,
            len(predecessor.content),
        )
    }
    for observation in observations:
        stage = observation.release_stage
        comparison_period = (
            _previous_quarter(observation.reference_period)
            if stage is EconomicReleaseStage.ADVANCE
            else observation.reference_period
        )
        previous = last_by_period.get(comparison_period)
        if previous is None:
            raise ValueError(
                "BEA GDP release has no preceding lineage artifact"
            )
        (
            previous_value,
            previous_lexical,
            previous_uri,
            previous_sha,
            previous_length,
        ) = previous
        if stage is EconomicReleaseStage.ADVANCE:
            revised_value = cast(float, observation.prior_period_value)
            revised_lexical = cast(str, observation.prior_period_lexical)
            comparison_kind = "prior-quarter"
        else:
            revised_value = observation.actual_value
            revised_lexical = observation.actual_lexical
            comparison_kind = "prior-stage"
        entries.append(
            BeaGdpArchiveEntryV1(
                observation=observation,
                comparison_reference_period=comparison_period,
                comparison_kind=comparison_kind,
                previous_artifact_uri=previous_uri,
                previous_content_sha256=previous_sha,
                previous_content_length=previous_length,
                previous_as_known_value=previous_value,
                previous_as_known_lexical=previous_lexical,
                revised_previous_value=revised_value,
                revised_previous_lexical=revised_lexical,
                normalized_sha256=_normalized_sha256(
                    observation,
                    comparison_period,
                    previous_lexical,
                    revised_lexical,
                ),
            )
        )
        current_snapshot = snapshots_by_uri[observation.artifact_uri]
        last_by_period[observation.reference_period] = (
            observation.actual_value,
            observation.actual_lexical,
            observation.artifact_uri,
            observation.content_sha256,
            len(current_snapshot.content),
        )
    values = tuple(entries)
    support = snapshots_by_uri[BEA_GDP_TIME_SUPPORT_URI]
    return BeaGdpArchiveManifestV1(
        registry_id=registry.registry_id,
        profile_id=profile.profile_id,
        release_index=release_index,
        predecessor_uri=BEA_GDP_PREDECESSOR_URI,
        predecessor_content_sha256=predecessor.content_sha256,
        predecessor_content_length=len(predecessor.content),
        predecessor_actual_value=predecessor_value,
        predecessor_actual_lexical=predecessor_lexical,
        entries=values,
        raw_artifact_count=len(values) + 2,
        total_content_bytes=(
            sum(item.observation.content_length for item in values)
            + len(predecessor.content)
            + len(support.content)
        ),
        advance_release_count=sum(
            item.release_stage is EconomicReleaseStage.ADVANCE
            for item in values
        ),
        second_release_count=sum(
            item.release_stage is EconomicReleaseStage.SECOND for item in values
        ),
        final_release_count=sum(
            item.release_stage is EconomicReleaseStage.FINAL for item in values
        ),
        nonzero_revision_count=sum(item.value_was_revised for item in values),
        advance_revision_count=sum(
            item.value_was_revised
            and item.release_stage is EconomicReleaseStage.ADVANCE
            for item in values
        ),
        second_revision_count=sum(
            item.value_was_revised
            and item.release_stage is EconomicReleaseStage.SECOND
            for item in values
        ),
        final_revision_count=sum(
            item.value_was_revised
            and item.release_stage is EconomicReleaseStage.FINAL
            for item in values
        ),
        historical_update_notice_count=sum(
            item.observation.historical_update_notice for item in values
        ),
        correction_notice_count=sum(
            item.observation.correction_notice for item in values
        ),
        shutdown_replacement_count=sum(
            item.observation.shutdown_replacement for item in values
        ),
        shutdown_disruption_quarter_count=_shutdown_disruption_count(
            release_index.releases
        ),
        fixed_width_release_count=sum(
            item.observation.source_era == "fixed-width-preformatted-html"
            for item in values
        ),
        narrative_release_count=sum(
            item.observation.source_era == "narrative-html" for item in values
        ),
        comparison_table_release_count=sum(
            item.observation.source_era == "comparison-table-html"
            for item in values
        ),
        zone_mismatch_count=sum(
            not item.observation.zone_consistent for item in values
        ),
        archive_date_mismatch_count=sum(
            item.release_date != item.archive_published_date
            for item in release_index.releases
        ),
    )


def replay_bea_gdp_archive(
    registry: OfficialSourceRegistryV1,
    profile: UnitedStatesBackfillProfileV1,
    index_snapshots: Sequence[OfficialRawSnapshotV1],
    snapshots_by_uri: Mapping[str, OfficialRawSnapshotV1],
    expected: BeaGdpArchiveManifestV1,
) -> BeaGdpArchiveManifestV1:
    """Recompute and compare the entire GDP archive receipt."""
    if not isinstance(expected, BeaGdpArchiveManifestV1):
        raise TypeError("BEA GDP replay requires a v1 expected manifest")
    release_index = parse_bea_gdp_release_index(
        index_snapshots, as_of_date=expected.release_index.as_of_date
    )
    rebuilt = build_bea_gdp_archive_manifest(
        registry, profile, release_index, snapshots_by_uri
    )
    if rebuilt != expected:
        raise ValueError("BEA GDP retained-corpus replay differs")
    return rebuilt


def bea_gdp_coverage_from_manifest(
    manifest: BeaGdpArchiveManifestV1,
) -> UnitedStatesProgramCoverageV1:
    """Derive the complete U.S. GDP coverage slice from a manifest."""
    if not isinstance(manifest, BeaGdpArchiveManifestV1):
        raise TypeError("BEA GDP coverage requires a v1 manifest")
    return UnitedStatesProgramCoverageV1(
        program_key=BEA_GDP_PROGRAM_KEY,
        window_start_date=US_BACKFILL_START_DATE,
        window_end_date=manifest.release_index.as_of_date,
        expected_occurrence_count=len(manifest.entries),
        schedule_count=len(manifest.entries),
        initial_actual_count=len(manifest.entries),
        previous_as_known_count=len(manifest.entries),
        revision_count=manifest.nonzero_revision_count,
        exact_minute_count=len(manifest.entries),
        forecast_count=0,
        artifact_sha256s=tuple(
            sorted(
                {
                    manifest.predecessor_content_sha256,
                    *(
                        item.observation.content_sha256
                        for item in manifest.entries
                    ),
                    *(
                        item.observation.release_time_support_sha256
                        for item in manifest.entries
                        if item.observation.release_time_support_sha256
                        is not None
                    ),
                }
            )
        ),
        gap_reasons=(),
        notes=(
            "GDP advance, second/preliminary, and third/final estimates remain separate occurrence identities.",
            "Two federal-shutdown quarters published initial and updated estimates instead of all three standard stages.",
            "SPF is a separate quarterly forecast product and is not projected into an event-level consensus.",
        ),
    )


def packaged_bea_gdp_archive_manifest_path() -> Path:
    """Return the packaged GDP manifest path."""
    return Path(__file__).with_name("assets") / "us_gdp_archive_v1.json"


def packaged_bea_gdp_index_path() -> Path:
    """Return the packaged multi-page GDP index envelope path."""
    return Path(__file__).with_name("assets") / "us_gdp_release_index_v1.json"


def load_packaged_bea_gdp_archive_manifest() -> BeaGdpArchiveManifestV1:
    """Load and validate the packaged GDP archive receipt."""
    return BeaGdpArchiveManifestV1.from_json(
        packaged_bea_gdp_archive_manifest_path().read_text(encoding="utf-8")
    )


def load_packaged_bea_gdp_index_snapshots() -> (
    tuple[OfficialRawSnapshotV1, ...]
):
    """Load exact archive pages from the packaged base64 JSON envelope."""
    payload = _mapping(
        json.loads(packaged_bea_gdp_index_path().read_text(encoding="utf-8")),
        "BEA GDP index envelope",
    )
    if payload.get("schema_version") != BEA_GDP_INDEX_ENVELOPE_SCHEMA_VERSION:
        raise ValueError("unsupported BEA GDP index-envelope schema")
    captured_at_ns = cast(int, payload.get("captured_at_ns"))
    _positive_int(captured_at_ns, "captured_at_ns", 2**63 - 1)
    registry = load_packaged_official_source_registry()
    requests = build_bea_gdp_index_requests(
        registry,
        page_count=len(_sequence(payload.get("pages"), "pages")),
    )
    snapshots: list[OfficialRawSnapshotV1] = []
    for request, item in zip(
        requests, _sequence(payload.get("pages"), "pages"), strict=True
    ):
        page = _mapping(item, "BEA GDP index-envelope page")
        if (
            cast(int, page.get("page_number")) != request.page_number
            or str(page.get("uri", "")) != request.uri
        ):
            raise ValueError("BEA GDP index-envelope page identity differs")
        try:
            content = base64.b64decode(
                str(page.get("content_base64", "")), validate=True
            )
        except (ValueError, TypeError) as exc:
            raise ValueError(
                "BEA GDP index envelope has invalid base64"
            ) from exc
        snapshots.append(
            OfficialRawSnapshotV1(
                request=request,
                retrieved_at_ns=captured_at_ns,
                completed_at_ns=captured_at_ns + 1,
                status_code=200,
                resolved_uri=request.uri,
                response_headers={"Content-Type": "text/html; charset=UTF-8"},
                content=content,
                content_type="text/html",
            )
        )
    return _validate_index_snapshots(snapshots)


def load_packaged_bea_gdp_index() -> BeaGdpReleaseIndexV1:
    """Parse the exact packaged GDP archive-page evidence."""
    manifest = load_packaged_bea_gdp_archive_manifest()
    return parse_bea_gdp_release_index(
        load_packaged_bea_gdp_index_snapshots(),
        as_of_date=manifest.release_index.as_of_date,
    )


__all__ = [
    "BEA_GDP_ARCHIVE_DATE_CORRECTIONS",
    "BEA_GDP_ARCHIVE_ENTRY_SCHEMA_VERSION",
    "BEA_GDP_ARCHIVE_MANIFEST_SCHEMA_VERSION",
    "BEA_GDP_ARCHIVE_PAGE_COUNT",
    "BEA_GDP_INDEX_ENTRY_SCHEMA_VERSION",
    "BEA_GDP_INDEX_ENVELOPE_SCHEMA_VERSION",
    "BEA_GDP_INDEX_PAGE_SCHEMA_VERSION",
    "BEA_GDP_INDEX_SCHEMA_VERSION",
    "BEA_GDP_INDEX_URI",
    "BEA_GDP_MEASURE_KEY",
    "BEA_GDP_OBSERVATION_SCHEMA_VERSION",
    "BEA_GDP_PREDECESSOR_DATE",
    "BEA_GDP_PREDECESSOR_URI",
    "BEA_GDP_PROGRAM_KEY",
    "BEA_GDP_SOURCE_KEY",
    "BEA_GDP_SUPPLEMENTAL_DATE",
    "BEA_GDP_SUPPLEMENTAL_TITLE",
    "BEA_GDP_SUPPLEMENTAL_URI",
    "BEA_GDP_TIME_SUPPORT_DATE",
    "BEA_GDP_TIME_SUPPORT_URI",
    "BeaGdpArchiveEntryV1",
    "BeaGdpArchiveManifestV1",
    "BeaGdpReleaseIndexEntryV1",
    "BeaGdpReleaseIndexPageV1",
    "BeaGdpReleaseIndexV1",
    "BeaGdpReleaseObservationV1",
    "bea_gdp_coverage_from_manifest",
    "build_bea_gdp_archive_manifest",
    "build_bea_gdp_archive_requests",
    "build_bea_gdp_index_requests",
    "load_packaged_bea_gdp_archive_manifest",
    "load_packaged_bea_gdp_index",
    "load_packaged_bea_gdp_index_snapshots",
    "packaged_bea_gdp_archive_manifest_path",
    "packaged_bea_gdp_index_path",
    "parse_bea_gdp_release",
    "parse_bea_gdp_release_index",
    "replay_bea_gdp_archive",
]
