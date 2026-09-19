"""Deterministic qualification of Eurostat monthly unemployment releases.

Eurostat's searchable Euro-indicator feed is the release inventory, while
source-dated HTML/PDF products are the vintage evidence.  The current
``une_rt_m`` JSON-stat table is retained only as a revised-series cross-check;
it never substitutes for values as published in a release.
"""

from __future__ import annotations

import hashlib
import html
import json
import math
import re
from calendar import monthrange
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from enum import Enum
from html.parser import HTMLParser
from io import BytesIO
from itertools import chain, pairwise
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit
from xml.etree.ElementTree import Element, ParseError

from defusedxml.common import DefusedXmlException
from defusedxml.ElementTree import fromstring as safe_xml_fromstring
from pypdf import PdfReader
from pypdf.errors import PdfReadError

from histdatacom.market_context._source_timestamps import (
    parse_source_iso_datetime,
)
from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
    load_packaged_official_source_registry,
)
from histdatacom.runtime_contracts import JSONValue

EUROSTAT_UNEMPLOYMENT_SOURCE_KEY: Final = "ea.eurostat.unemployment"
EUROSTAT_UNEMPLOYMENT_PROGRAM_KEY: Final = "ea.eurostat.monthly-unemployment"
EUROSTAT_UNEMPLOYMENT_MEASURE_KEY: Final = "unemployment-rate-total-sa"
EUROSTAT_UNEMPLOYMENT_PARSER_ID: Final = "official.eurostat-unemployment.v1"
EUROSTAT_UNEMPLOYMENT_PARSER_VERSION: Final = "1"
EUROSTAT_UNEMPLOYMENT_SOURCE_TIMEZONE: Final = "Europe/Brussels"
EUROSTAT_UNEMPLOYMENT_AS_OF_DATE: Final = "2026-09-17"
EUROSTAT_UNEMPLOYMENT_FIRST_SEARCH_RELEASE_DATE: Final = "2002-01-04"
EUROSTAT_UNEMPLOYMENT_LATEST_PACKAGED_RELEASE_DATE: Final = "2026-09-01"
EUROSTAT_UNEMPLOYMENT_FIRST_REFERENCE_PERIOD: Final = "2001-11"
EUROSTAT_UNEMPLOYMENT_LATEST_REFERENCE_PERIOD: Final = "2026-07"
EUROSTAT_UNEMPLOYMENT_INTERNAL_GAP_REFERENCE_PERIODS: Final = ("2003-02",)
EUROSTAT_UNEMPLOYMENT_PAGE_DATE_OFFSET_EXCEPTIONS: Final = {
    "3-30032007-bp": 3,
}
EUROSTAT_UNEMPLOYMENT_SEARCH_PAGE_COUNT: Final = 4
EUROSTAT_UNEMPLOYMENT_SEARCH_RESULT_COUNT: Final = 383
EUROSTAT_UNEMPLOYMENT_TITLE_MATCH_COUNT: Final = 317
EUROSTAT_UNEMPLOYMENT_PREFILTER_COUNT: Final = 296
EUROSTAT_UNEMPLOYMENT_RELEASE_COUNT: Final = 296
EUROSTAT_UNEMPLOYMENT_DOCUMENT_COUNT: Final = 264

EUROSTAT_UNEMPLOYMENT_SEARCH_URI: Final = "https://ec.europa.eu/eurostat/search"
EUROSTAT_UNEMPLOYMENT_PRODUCT_URI_TEMPLATE: Final = (
    "https://ec.europa.eu/eurostat/product?code={product_code}"
)
EUROSTAT_UNEMPLOYMENT_DATASET_URI: Final = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "une_rt_m?lang=en&freq=M&s_adj=SA&age=TOTAL&unit=PC_ACT&sex=T&"
    "geo=EA21&geo=DE&geo=FR&sinceTimePeriod=2000-01"
)

EUROSTAT_UNEMPLOYMENT_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-unemployment-artifact.v1"
)
EUROSTAT_UNEMPLOYMENT_INVENTORY_ENTRY_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-unemployment-inventory-entry.v1"
)
EUROSTAT_UNEMPLOYMENT_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-unemployment-exclusion.v1"
)
EUROSTAT_UNEMPLOYMENT_INVENTORY_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-unemployment-inventory.v1"
)
EUROSTAT_UNEMPLOYMENT_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-unemployment-observation.v1"
)
EUROSTAT_UNEMPLOYMENT_DATASET_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-unemployment-dataset.v1"
)
EUROSTAT_UNEMPLOYMENT_RELEASE_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-unemployment-release-value.v1"
)
EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-unemployment-published-value.v1"
)
EUROSTAT_UNEMPLOYMENT_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-unemployment-release.v1"
)
EUROSTAT_UNEMPLOYMENT_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-unemployment-archive-manifest.v1"
)

MAX_EUROSTAT_UNEMPLOYMENT_INDEX_PAGES: Final = 32
MAX_EUROSTAT_UNEMPLOYMENT_SEARCH_ENTRIES: Final = 2_048
MAX_EUROSTAT_UNEMPLOYMENT_RELEASES: Final = 512
MAX_EUROSTAT_UNEMPLOYMENT_OBSERVATIONS: Final = 4_096
MAX_EUROSTAT_UNEMPLOYMENT_ARTIFACTS: Final = 2_048
MAX_EUROSTAT_UNEMPLOYMENT_TOTAL_BYTES: Final = 1_024_000_000
MAX_EUROSTAT_UNEMPLOYMENT_TITLE_CHARS: Final = 512
MAX_EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUES_PER_RELEASE: Final = 39

_SEARCH_PREFIX: Final = (
    "_estatsearchportlet_WAR_estatsearchportlet_INSTANCE_bHVzuvn1SZ8J_"
)
_ATOM_NAMESPACE: Final = "http://www.w3.org/2005/Atom"
_PRODUCT_CODE_RE = re.compile(
    r"(?P<prefix>[1234])-(?P<day>\d{2})(?P<month>\d{2})(?P<year>\d{4})-"
    r"(?P<suffix>ap(?:\d+|_\d+)?|bp\d*|cp\d*)",
    re.IGNORECASE,
)
_MONTH_RE = re.compile(r"\d{4}-(?:0[1-9]|1[0-2])")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")
_SIGNED_NUMBER_RE = re.compile(r"[+-]?\d+(?:\.\d+)?")
_SPACE_RE = re.compile(r"\s+")
_ECONOMIES: Final = ("EA21", "DE", "FR")
_ECONOMY_ORDER: Final = {code: index for index, code in enumerate(_ECONOMIES)}
_RELEASE_ECONOMIES: Final = ("EA", "DE", "FR")
_RELEASE_ECONOMY_ORDER: Final = {
    code: index for index, code in enumerate(_RELEASE_ECONOMIES)
}


def _required_text(value: object, name: str, *, maximum: int = 16_384) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    result = _SPACE_RE.sub(" ", value).strip()
    if not result:
        raise ValueError(f"{name} is required")
    if len(result) > maximum:
        raise ValueError(f"{name} exceeds its bound")
    return result


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, name)


def _bounded_int(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
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


def _https_uri(value: object, name: str) -> str:
    uri = _required_text(value, name)
    parsed = urlsplit(uri)
    if parsed.scheme != "https" or parsed.netloc != "ec.europa.eu":
        raise ValueError(f"{name} must be an allowlisted Eurostat HTTPS URI")
    if (
        parsed.username
        or parsed.password
        or not parsed.path.startswith("/eurostat")
    ):
        raise ValueError(f"{name} has an unsupported authority or path")
    return uri


def _iso_date(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        parsed = date.fromisoformat(result)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date") from exc
    if parsed.isoformat() != result:
        raise ValueError(f"{name} is not canonical")
    return result


def _iso_datetime(value: object, name: str) -> str:
    result = _required_text(value, name)
    try:
        parsed = parse_source_iso_datetime(result.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include an offset")
    return result


def _month(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _MONTH_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-MM")
    if date.fromisoformat(f"{result}-01").strftime("%Y-%m") != result:
        raise ValueError(f"{name} is not canonical")
    return result


def _month_shift(value: str, offset: int) -> str:
    token = _month(value, "month")
    year, month = (int(item) for item in token.split("-"))
    ordinal = year * 12 + month - 1 + offset
    return f"{ordinal // 12:04d}-{ordinal % 12 + 1:02d}"


def _month_range(start: str, end: str) -> tuple[str, ...]:
    first = _month(start, "start")
    last = _month(end, "end")
    if last < first:
        return ()
    result: list[str] = []
    current = first
    while current <= last:
        result.append(current)
        current = _month_shift(current, 1)
    return tuple(result)


def _sha256(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _SHA256_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return result


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _product_code(uri_or_code: object) -> str:
    value = _required_text(uri_or_code, "product_code")
    parsed = urlsplit(value)
    if parsed.scheme and parsed.path == "/eurostat/product":
        candidates = parse_qs(parsed.query, keep_blank_values=True).get(
            "code", []
        )
        if len(candidates) != 1:
            raise ValueError(
                "Eurostat unemployment product URI has no unique product code"
            )
        token = candidates[0]
    else:
        token = parsed.path if parsed.scheme else value
    match = _PRODUCT_CODE_RE.search(token)
    if match is None:
        raise ValueError("Eurostat unemployment product code is invalid")
    return match.group(0).casefold()


def _product_release_date(product_code: object) -> str:
    match = _PRODUCT_CODE_RE.fullmatch(_product_code(product_code))
    if match is None:  # pragma: no cover - guarded by _product_code
        raise ValueError("Eurostat unemployment product code is invalid")
    token = f"{match.group('year')}-{match.group('month')}-{match.group('day')}"
    return _iso_date(token, "release_date")


def _inventory_prefilter(title: str) -> bool:
    normalized = _required_text(title, "title").casefold()
    return (
        normalized.startswith(
            (
                "euro area unemployment",
                "euro-zone unemployment",
                "euro-zone and eu",
                "euro area and eu",
            )
        )
        and "government deficit" not in normalized
    )


def _inventory_selected(title: str, product_code: str) -> bool:
    _product_code(product_code)
    return _inventory_prefilter(title)


def _exclusion_reason(title: str) -> str:
    normalized = _required_text(title, "title").casefold()
    if "regional" in normalized or "regions" in normalized:
        return "regional unemployment article, not the monthly headline release"
    if "unemployment" not in normalized:
        return "search-summary match without unemployment in the title"
    return "thematic labour article, not the monthly headline release"


class EurostatUnemploymentArtifactRole(str, Enum):
    """Role of one exact official byte artifact."""

    SEARCH_INDEX = "search-index"
    CURRENT_DATASET = "current-dataset"
    RELEASE_HTML = "release-html"
    RELEASE_DOCUMENT_HTML = "release-document-html"
    RELEASE_DOCUMENT_PDF = "release-document-pdf"

    @classmethod
    def from_value(
        cls, value: str | EurostatUnemploymentArtifactRole
    ) -> EurostatUnemploymentArtifactRole:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat unemployment artifact role"
            ) from exc


class EurostatUnemploymentMeasure(str, Enum):
    """Qualified unemployment statistic carried by this archive."""

    RATE_PERCENT_ACTIVE = "rate-percent-active-population"

    @classmethod
    def from_value(
        cls, value: str | EurostatUnemploymentMeasure
    ) -> EurostatUnemploymentMeasure:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat unemployment measure"
            ) from exc


@dataclass(frozen=True, slots=True)
class EurostatUnemploymentArtifactV1:
    """Content-addressed official unemployment evidence artifact."""

    role: EurostatUnemploymentArtifactRole
    request_uri: str
    resolved_uri: str
    source_format: OfficialSourceFormat
    content_length: int
    content_sha256: str
    page_number: int | None = None
    product_code: str | None = None
    schema_version: str = EUROSTAT_UNEMPLOYMENT_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_UNEMPLOYMENT_ARTIFACT_SCHEMA_VERSION:
            raise ValueError(
                "unsupported Eurostat unemployment artifact schema"
            )
        role = EurostatUnemploymentArtifactRole.from_value(self.role)
        request_uri = _https_uri(self.request_uri, "request_uri")
        resolved_uri = _https_uri(self.resolved_uri, "resolved_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        expected_format = {
            EurostatUnemploymentArtifactRole.SEARCH_INDEX: OfficialSourceFormat.ATOM,
            EurostatUnemploymentArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
            EurostatUnemploymentArtifactRole.RELEASE_HTML: OfficialSourceFormat.HTML,
            EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
            EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
        }[role]
        if source_format is not expected_format:
            raise ValueError(
                "Eurostat unemployment artifact role and format differ"
            )
        length = _bounded_int(
            self.content_length,
            "content_length",
            MAX_EUROSTAT_UNEMPLOYMENT_TOTAL_BYTES,
        )
        if length < 1:
            raise ValueError("Eurostat unemployment artifact is empty")
        page_number = self.page_number
        product_code = self.product_code
        if role is EurostatUnemploymentArtifactRole.SEARCH_INDEX:
            if page_number is None:
                raise ValueError(
                    "Eurostat unemployment search artifact needs a page number"
                )
            _bounded_int(
                page_number,
                "page_number",
                MAX_EUROSTAT_UNEMPLOYMENT_INDEX_PAGES,
            )
            if product_code is not None:
                raise ValueError(
                    "Eurostat unemployment search artifact has a product code"
                )
        elif role is EurostatUnemploymentArtifactRole.CURRENT_DATASET:
            if page_number is not None or product_code is not None:
                raise ValueError(
                    "Eurostat unemployment dataset artifact has release metadata"
                )
        else:
            if page_number is not None or product_code is None:
                raise ValueError(
                    "Eurostat unemployment release artifact metadata differs"
                )
            product_code = _product_code(product_code)
        object.__setattr__(self, "role", role)
        object.__setattr__(self, "request_uri", request_uri)
        object.__setattr__(self, "resolved_uri", resolved_uri)
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "content_length", length)
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, "content_sha256"),
        )
        object.__setattr__(self, "product_code", product_code)

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role.value,
            "request_uri": self.request_uri,
            "resolved_uri": self.resolved_uri,
            "source_format": self.source_format.value,
            "content_length": self.content_length,
            "content_sha256": self.content_sha256,
            "page_number": self.page_number,
            "product_code": self.product_code,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatUnemploymentArtifactV1:
        return cls(
            role=EurostatUnemploymentArtifactRole.from_value(
                str(data.get("role", ""))
            ),
            request_uri=str(data.get("request_uri", "")),
            resolved_uri=str(data.get("resolved_uri", "")),
            source_format=OfficialSourceFormat.from_value(
                str(data.get("source_format", ""))
            ),
            content_length=cast(int, data.get("content_length")),
            content_sha256=str(data.get("content_sha256", "")),
            page_number=cast(int | None, data.get("page_number")),
            product_code=_optional_text(
                data.get("product_code"), "product_code"
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatUnemploymentInventoryEntryV1:
    """One selected entry from the official Eurostat Atom inventory."""

    product_code: str
    release_date: str
    source_title: str
    source_uri: str
    atom_published_at: str
    atom_updated_at: str
    summary: str
    page_number: int
    entry_id: str = ""
    schema_version: str = EUROSTAT_UNEMPLOYMENT_INVENTORY_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_UNEMPLOYMENT_INVENTORY_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat unemployment inventory-entry schema"
            )
        product_code = _product_code(self.product_code)
        release_date = _iso_date(self.release_date, "release_date")
        if release_date != _product_release_date(product_code):
            raise ValueError(
                "Eurostat unemployment release date differs from product code"
            )
        title = _required_text(
            self.source_title,
            "source_title",
            maximum=MAX_EUROSTAT_UNEMPLOYMENT_TITLE_CHARS,
        )
        if not _inventory_selected(title, product_code):
            raise ValueError(
                "Eurostat unemployment inventory entry is outside selection"
            )
        source_uri = _https_uri(self.source_uri, "source_uri")
        parsed_source_uri = urlsplit(source_uri)
        if (
            parsed_source_uri.path != "/eurostat/product"
            or set(parse_qs(parsed_source_uri.query, keep_blank_values=True))
            != {"code"}
            or parsed_source_uri.fragment
        ):
            raise ValueError("Eurostat unemployment source URI route differs")
        if _product_code(source_uri) != product_code:
            raise ValueError(
                "Eurostat unemployment source URI and product code differ"
            )
        page = _bounded_int(
            self.page_number,
            "page_number",
            MAX_EUROSTAT_UNEMPLOYMENT_INDEX_PAGES,
        )
        if page < 1:
            raise ValueError(
                "Eurostat unemployment page number must be positive"
            )
        object.__setattr__(self, "product_code", product_code)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "source_uri", source_uri)
        object.__setattr__(
            self,
            "atom_published_at",
            _iso_datetime(self.atom_published_at, "atom_published_at"),
        )
        object.__setattr__(
            self,
            "atom_updated_at",
            _iso_datetime(self.atom_updated_at, "atom_updated_at"),
        )
        object.__setattr__(
            self,
            "summary",
            _required_text(self.summary, "summary", maximum=8_192),
        )
        expected = _stable_id(
            "eurostat-unemployment-inventory-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError(
                "Eurostat unemployment inventory-entry identity differs"
            )
        object.__setattr__(self, "entry_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "product_code": self.product_code,
            "release_date": self.release_date,
            "source_title": self.source_title,
            "source_uri": self.source_uri,
            "atom_published_at": self.atom_published_at,
            "atom_updated_at": self.atom_updated_at,
            "summary": self.summary,
            "page_number": self.page_number,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "entry_id": self.entry_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatUnemploymentInventoryEntryV1:
        return cls(
            product_code=str(data.get("product_code", "")),
            release_date=str(data.get("release_date", "")),
            source_title=str(data.get("source_title", "")),
            source_uri=str(data.get("source_uri", "")),
            atom_published_at=str(data.get("atom_published_at", "")),
            atom_updated_at=str(data.get("atom_updated_at", "")),
            summary=str(data.get("summary", "")),
            page_number=cast(int, data.get("page_number")),
            entry_id=str(data.get("entry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatUnemploymentIndexExclusionV1:
    """One explicit non-lineage result from the bounded Atom search."""

    product_code: str
    release_date: str
    source_title: str
    source_uri: str
    reason: str
    exclusion_id: str = ""
    schema_version: str = EUROSTAT_UNEMPLOYMENT_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_UNEMPLOYMENT_EXCLUSION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat unemployment exclusion schema"
            )
        product_code = _product_code(self.product_code)
        release_date = _iso_date(self.release_date, "release_date")
        if release_date != _product_release_date(product_code):
            raise ValueError("Eurostat unemployment exclusion date differs")
        title = _required_text(
            self.source_title,
            "source_title",
            maximum=MAX_EUROSTAT_UNEMPLOYMENT_TITLE_CHARS,
        )
        if _inventory_selected(title, product_code):
            raise ValueError(
                "Eurostat unemployment exclusion matches selection"
            )
        source_uri = _https_uri(self.source_uri, "source_uri")
        if _product_code(source_uri) != product_code:
            raise ValueError("Eurostat unemployment exclusion URI differs")
        reason = _required_text(self.reason, "reason")
        if reason != _exclusion_reason(title):
            raise ValueError("Eurostat unemployment exclusion reason differs")
        object.__setattr__(self, "product_code", product_code)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "source_uri", source_uri)
        object.__setattr__(self, "reason", reason)
        expected = _stable_id(
            "eurostat-unemployment-exclusion", self.identity_payload()
        )
        if self.exclusion_id and self.exclusion_id != expected:
            raise ValueError("Eurostat unemployment exclusion identity differs")
        object.__setattr__(self, "exclusion_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "product_code": self.product_code,
            "release_date": self.release_date,
            "source_title": self.source_title,
            "source_uri": self.source_uri,
            "reason": self.reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "exclusion_id": self.exclusion_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatUnemploymentIndexExclusionV1:
        return cls(
            product_code=str(data.get("product_code", "")),
            release_date=str(data.get("release_date", "")),
            source_title=str(data.get("source_title", "")),
            source_uri=str(data.get("source_uri", "")),
            reason=str(data.get("reason", "")),
            exclusion_id=str(data.get("exclusion_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatUnemploymentInventoryV1:
    """Hash-bound official search inventory and deterministic selection."""

    artifacts: tuple[EurostatUnemploymentArtifactV1, ...]
    entries: tuple[EurostatUnemploymentInventoryEntryV1, ...]
    exclusions: tuple[EurostatUnemploymentIndexExclusionV1, ...]
    query_result_count: int
    title_match_count: int
    prefilter_count: int
    inventory_id: str = ""
    schema_version: str = EUROSTAT_UNEMPLOYMENT_INVENTORY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_UNEMPLOYMENT_INVENTORY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat unemployment inventory schema"
            )
        artifacts = tuple(self.artifacts)
        entries = tuple(self.entries)
        exclusions = tuple(self.exclusions)
        if len(artifacts) != EUROSTAT_UNEMPLOYMENT_SEARCH_PAGE_COUNT:
            raise ValueError("Eurostat unemployment search page count differs")
        if any(
            item.role is not EurostatUnemploymentArtifactRole.SEARCH_INDEX
            for item in artifacts
        ):
            raise ValueError(
                "Eurostat unemployment inventory artifact role differs"
            )
        if [item.page_number for item in artifacts] != list(
            range(1, EUROSTAT_UNEMPLOYMENT_SEARCH_PAGE_COUNT + 1)
        ):
            raise ValueError(
                "Eurostat unemployment search pages are not contiguous"
            )
        if any(
            item.request_uri != _search_uri(cast(int, item.page_number))
            or item.resolved_uri != item.request_uri
            for item in artifacts
        ):
            raise ValueError(
                "Eurostat unemployment search artifact identity differs"
            )
        if len(entries) != EUROSTAT_UNEMPLOYMENT_RELEASE_COUNT:
            raise ValueError(
                "Eurostat unemployment selected release count differs"
            )
        codes = [item.product_code for item in entries]
        if len(codes) != len(set(codes)):
            raise ValueError(
                "Eurostat unemployment inventory repeats a product code"
            )
        if list(entries) != sorted(
            entries, key=lambda item: (item.release_date, item.product_code)
        ):
            raise ValueError(
                "Eurostat unemployment inventory entries are not canonical"
            )
        if (
            entries[0].release_date
            != EUROSTAT_UNEMPLOYMENT_FIRST_SEARCH_RELEASE_DATE
        ):
            raise ValueError(
                "Eurostat unemployment first searchable release differs"
            )
        if (
            entries[-1].release_date
            != EUROSTAT_UNEMPLOYMENT_LATEST_PACKAGED_RELEASE_DATE
        ):
            raise ValueError(
                "Eurostat unemployment latest searchable release differs"
            )
        if len(exclusions) != (
            EUROSTAT_UNEMPLOYMENT_SEARCH_RESULT_COUNT
            - EUROSTAT_UNEMPLOYMENT_RELEASE_COUNT
        ):
            raise ValueError("Eurostat unemployment exclusion count differs")
        exclusion_codes = [item.product_code for item in exclusions]
        if len(exclusion_codes) != len(set(exclusion_codes)):
            raise ValueError(
                "Eurostat unemployment exclusions repeat a product"
            )
        if list(exclusions) != sorted(
            exclusions, key=lambda item: (item.release_date, item.product_code)
        ):
            raise ValueError(
                "Eurostat unemployment exclusions are not canonical"
            )
        if set(codes) & set(exclusion_codes):
            raise ValueError(
                "Eurostat unemployment selection includes an exclusion"
            )
        expected_counts = (
            EUROSTAT_UNEMPLOYMENT_SEARCH_RESULT_COUNT,
            EUROSTAT_UNEMPLOYMENT_TITLE_MATCH_COUNT,
            EUROSTAT_UNEMPLOYMENT_PREFILTER_COUNT,
        )
        observed_counts = (
            _bounded_int(
                self.query_result_count,
                "query_result_count",
                MAX_EUROSTAT_UNEMPLOYMENT_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.title_match_count,
                "title_match_count",
                MAX_EUROSTAT_UNEMPLOYMENT_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.prefilter_count,
                "prefilter_count",
                MAX_EUROSTAT_UNEMPLOYMENT_SEARCH_ENTRIES,
            ),
        )
        if observed_counts != expected_counts:
            raise ValueError(
                "Eurostat unemployment search inventory counts differ"
            )
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "exclusions", exclusions)
        expected = _stable_id(
            "eurostat-unemployment-inventory", self.identity_payload()
        )
        if self.inventory_id and self.inventory_id != expected:
            raise ValueError("Eurostat unemployment inventory identity differs")
        object.__setattr__(self, "inventory_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "artifacts": [item.to_dict() for item in self.artifacts],
            "entries": [item.to_dict() for item in self.entries],
            "exclusions": [item.to_dict() for item in self.exclusions],
            "query_result_count": self.query_result_count,
            "title_match_count": self.title_match_count,
            "prefilter_count": self.prefilter_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "inventory_id": self.inventory_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatUnemploymentInventoryV1:
        return cls(
            artifacts=tuple(
                EurostatUnemploymentArtifactV1.from_dict(
                    _mapping(item, "artifact")
                )
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            entries=tuple(
                EurostatUnemploymentInventoryEntryV1.from_dict(
                    _mapping(item, "inventory entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            exclusions=tuple(
                EurostatUnemploymentIndexExclusionV1.from_dict(
                    _mapping(item, "exclusion")
                )
                for item in _sequence(data.get("exclusions"), "exclusions")
            ),
            query_result_count=cast(int, data.get("query_result_count")),
            title_match_count=cast(int, data.get("title_match_count")),
            prefilter_count=cast(int, data.get("prefilter_count")),
            inventory_id=str(data.get("inventory_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatUnemploymentObservationV1:
    """One current-revised JSON-stat unemployment-rate observation."""

    economy_code: str
    reference_period: str
    measure: EurostatUnemploymentMeasure
    value_lexical: str
    value: float
    status: str | None
    flat_index: int
    observation_id: str = ""
    schema_version: str = EUROSTAT_UNEMPLOYMENT_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_UNEMPLOYMENT_OBSERVATION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat unemployment observation schema"
            )
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _ECONOMIES:
            raise ValueError(
                "Eurostat unemployment observation economy differs"
            )
        reference = _month(self.reference_period, "reference_period")
        measure = EurostatUnemploymentMeasure.from_value(self.measure)
        if measure is not EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE:
            raise ValueError(
                "Eurostat unemployment observation measure differs"
            )
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError(
                "Eurostat unemployment observation lexical is invalid"
            )
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError(
                "Eurostat unemployment observation numeric differs"
            )
        if not 0.0 <= numeric <= 100.0:
            raise ValueError(
                "Eurostat unemployment observation is outside its bound"
            )
        status = _optional_text(self.status, "status")
        if status is not None and len(status) > 16:
            raise ValueError("Eurostat unemployment status exceeds its bound")
        flat_index = _bounded_int(
            self.flat_index,
            "flat_index",
            MAX_EUROSTAT_UNEMPLOYMENT_OBSERVATIONS * 4,
        )
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "flat_index", flat_index)
        expected = _stable_id(
            "eurostat-unemployment-observation", self.identity_payload()
        )
        if self.observation_id and self.observation_id != expected:
            raise ValueError(
                "Eurostat unemployment observation identity differs"
            )
        object.__setattr__(self, "observation_id", expected)

    @property
    def key(self) -> tuple[str, str, str]:
        return self.measure.value, self.economy_code, self.reference_period

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "reference_period": self.reference_period,
            "measure": self.measure.value,
            "value_lexical": self.value_lexical,
            "value": self.value,
            "status": self.status,
            "flat_index": self.flat_index,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "observation_id": self.observation_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatUnemploymentObservationV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatUnemploymentMeasure.from_value(
                str(data.get("measure", ""))
            ),
            value_lexical=str(data.get("value_lexical", "")),
            value=float(cast(float, data.get("value"))),
            status=_optional_text(data.get("status"), "status"),
            flat_index=cast(int, data.get("flat_index")),
            observation_id=str(data.get("observation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatUnemploymentDatasetV1:
    """Exact current-revised ``une_rt_m`` cross-check response."""

    label: str
    updated_at: str
    time_start: str
    time_end: str
    artifact: EurostatUnemploymentArtifactV1
    observations: tuple[EurostatUnemploymentObservationV1, ...]
    dataset_id: str = "une_rt_m"
    dataset_receipt_id: str = ""
    schema_version: str = EUROSTAT_UNEMPLOYMENT_DATASET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_UNEMPLOYMENT_DATASET_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat unemployment dataset schema")
        if self.dataset_id != "une_rt_m":
            raise ValueError("unsupported Eurostat unemployment dataset")
        label = _required_text(self.label, "label")
        updated = _iso_datetime(self.updated_at, "updated_at")
        time_start = _month(self.time_start, "time_start")
        time_end = _month(self.time_end, "time_end")
        if time_end < time_start:
            raise ValueError(
                "Eurostat unemployment dataset time range is reversed"
            )
        if (
            self.artifact.role
            is not EurostatUnemploymentArtifactRole.CURRENT_DATASET
        ):
            raise ValueError(
                "Eurostat unemployment dataset artifact role differs"
            )
        if (
            self.artifact.request_uri != EUROSTAT_UNEMPLOYMENT_DATASET_URI
            or self.artifact.resolved_uri != EUROSTAT_UNEMPLOYMENT_DATASET_URI
        ):
            raise ValueError(
                "Eurostat unemployment dataset artifact identity differs"
            )
        observations = tuple(self.observations)
        if not 1 <= len(observations) <= MAX_EUROSTAT_UNEMPLOYMENT_OBSERVATIONS:
            raise ValueError(
                "Eurostat unemployment observation count is invalid"
            )
        keys = [item.key for item in observations]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "Eurostat unemployment dataset repeats an observation"
            )
        periods = _month_range(time_start, time_end)
        expected_keys = {
            (measure.value, economy, period)
            for measure in EurostatUnemploymentMeasure
            for economy in _ECONOMIES
            for period in periods
        }
        missing_keys = expected_keys - set(keys)
        expected_missing = {
            (
                EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE.value,
                economy,
                "2026-08",
            )
            for economy in _ECONOMIES
        }
        if missing_keys != expected_missing or set(keys) - expected_keys:
            raise ValueError(
                "Eurostat unemployment dataset cube coverage differs"
            )
        period_positions = {
            period: position for position, period in enumerate(periods)
        }
        if any(
            item.flat_index
            != _ECONOMY_ORDER[item.economy_code] * len(periods)
            + period_positions[item.reference_period]
            for item in observations
        ):
            raise ValueError(
                "Eurostat unemployment observation coordinate differs"
            )
        if list(observations) != sorted(
            observations,
            key=lambda item: (
                _ECONOMY_ORDER[item.economy_code],
                item.reference_period,
            ),
        ):
            raise ValueError(
                "Eurostat unemployment observations are not canonical"
            )
        if min(
            item.reference_period for item in observations
        ) != time_start or max(
            item.reference_period for item in observations
        ) != _month_shift(
            time_end, -1
        ):
            raise ValueError("Eurostat unemployment dataset time range differs")
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "updated_at", updated)
        object.__setattr__(self, "time_start", time_start)
        object.__setattr__(self, "time_end", time_end)
        object.__setattr__(self, "observations", observations)
        expected = _stable_id(
            "eurostat-unemployment-dataset", self.identity_payload()
        )
        if self.dataset_receipt_id and self.dataset_receipt_id != expected:
            raise ValueError("Eurostat unemployment dataset identity differs")
        object.__setattr__(self, "dataset_receipt_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "label": self.label,
            "updated_at": self.updated_at,
            "time_start": self.time_start,
            "time_end": self.time_end,
            "artifact": self.artifact.to_dict(),
            "observations": [item.to_dict() for item in self.observations],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "dataset_receipt_id": self.dataset_receipt_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatUnemploymentDatasetV1:
        return cls(
            label=str(data.get("label", "")),
            updated_at=str(data.get("updated_at", "")),
            time_start=str(data.get("time_start", "")),
            time_end=str(data.get("time_end", "")),
            artifact=EurostatUnemploymentArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            observations=tuple(
                EurostatUnemploymentObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            dataset_id=str(data.get("dataset_id", "")),
            dataset_receipt_id=str(data.get("dataset_receipt_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _search_uri(page_number: int) -> str:
    page = _bounded_int(
        page_number, "page_number", MAX_EUROSTAT_UNEMPLOYMENT_INDEX_PAGES
    )
    if page < 1:
        raise ValueError("Eurostat unemployment search page must be positive")
    params = {
        "p_p_id": (
            "estatsearchportlet_WAR_estatsearchportlet_INSTANCE_bHVzuvn1SZ8J"
        ),
        "p_p_lifecycle": "2",
        "p_p_state": "maximized",
        "p_p_mode": "view",
        "p_p_resource_id": "atom",
        "p_p_cacheability": "cacheLevelPage",
        f"{_SEARCH_PREFIX}pageNumber": "1",
        f"{_SEARCH_PREFIX}pageSize": "100",
        f"{_SEARCH_PREFIX}text": "unemployment",
        f"{_SEARCH_PREFIX}sort": "date",
        f"{_SEARCH_PREFIX}collection": "CAT_PREREL",
        f"{_SEARCH_PREFIX}priv_r_p_implicitModel": "true",
        "pageNumber": str(page),
    }
    return f"{EUROSTAT_UNEMPLOYMENT_SEARCH_URI}?{urlencode(params)}"


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(EUROSTAT_UNEMPLOYMENT_SOURCE_KEY)
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=OfficialRequestMethod.GET,
        uri=uri,
        source_format=source_format,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
        page_number=page_number,
    )


def build_eurostat_unemployment_search_requests(
    registry: OfficialSourceRegistryV1,
    *,
    page_count: int = EUROSTAT_UNEMPLOYMENT_SEARCH_PAGE_COUNT,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the bounded official Atom inventory requests."""
    count = _bounded_int(
        page_count, "page_count", MAX_EUROSTAT_UNEMPLOYMENT_INDEX_PAGES
    )
    if count < 1:
        raise ValueError("Eurostat unemployment search needs at least one page")
    return tuple(
        _request(
            registry,
            _search_uri(page),
            OfficialSourceFormat.ATOM,
            page_number=page,
        )
        for page in range(1, count + 1)
    )


def build_eurostat_unemployment_dataset_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact current-revised unemployment cross-check request."""
    return _request(
        registry,
        EUROSTAT_UNEMPLOYMENT_DATASET_URI,
        OfficialSourceFormat.JSON_STAT,
    )


def build_eurostat_unemployment_release_requests(
    registry: OfficialSourceRegistryV1,
    inventory: EurostatUnemploymentInventoryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one primary request for every selected unemployment product."""
    if not isinstance(inventory, EurostatUnemploymentInventoryV1):
        raise TypeError(
            "Eurostat unemployment release requests require a v1 inventory"
        )
    return tuple(
        _request(registry, item.source_uri, OfficialSourceFormat.HTML)
        for item in inventory.entries
    )


def _document_source_format(uri: str) -> OfficialSourceFormat:
    path = urlsplit(_https_uri(uri, "document_uri")).path.casefold()
    if re.search(r"\.pdf(?:\.|/|$)", path):
        return OfficialSourceFormat.PDF
    if re.search(r"\.html?(?:\.|/|$)", path):
        return OfficialSourceFormat.HTML
    raise ValueError("Eurostat unemployment release document format differs")


def build_eurostat_unemployment_document_requests(
    registry: OfficialSourceRegistryV1,
    inventory: EurostatUnemploymentInventoryV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Discover one English source document for each legacy landing."""
    if not isinstance(inventory, EurostatUnemploymentInventoryV1):
        raise TypeError(
            "Eurostat unemployment document requests require a v1 inventory"
        )
    entries = {item.product_code: item for item in inventory.entries}
    snapshots = tuple(release_snapshots)
    if len(snapshots) != EUROSTAT_UNEMPLOYMENT_RELEASE_COUNT:
        raise ValueError("Eurostat unemployment landing snapshot count differs")
    by_code: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        code = _product_code(snapshot.request.uri)
        if code in by_code:
            raise ValueError(
                "Eurostat unemployment landing corpus repeats a product code"
            )
        by_code[code] = snapshot
    if set(by_code) != set(entries):
        raise ValueError("Eurostat unemployment landing corpus is incomplete")
    requests: list[OfficialSourceRequestV1] = []
    for code, entry in entries.items():
        snapshot = by_code[code]
        if snapshot.request.uri != entry.source_uri:
            raise ValueError(
                "Eurostat unemployment landing request identity differs"
            )
        _, _, _, document_uris = _parse_release_landing(snapshot, entry)
        requests.extend(
            _request(registry, uri, _document_source_format(uri))
            for uri in document_uris
        )
    if len(requests) != EUROSTAT_UNEMPLOYMENT_DOCUMENT_COUNT:
        raise ValueError("Eurostat unemployment English document count differs")
    return tuple(requests)


def _artifact_from_snapshot(
    snapshot: OfficialRawSnapshotV1,
    role: EurostatUnemploymentArtifactRole,
    *,
    product_code: str | None = None,
) -> EurostatUnemploymentArtifactV1:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("Eurostat unemployment artifact requires a v1 snapshot")
    if (
        snapshot.request.source_key != EUROSTAT_UNEMPLOYMENT_SOURCE_KEY
        or snapshot.request.parser_id != EUROSTAT_UNEMPLOYMENT_PARSER_ID
        or snapshot.request.parser_version
        != EUROSTAT_UNEMPLOYMENT_PARSER_VERSION
    ):
        raise ValueError(
            "Eurostat unemployment snapshot source or parser differs"
        )
    if snapshot.request.method is not OfficialRequestMethod.GET:
        raise ValueError("Eurostat unemployment snapshot method differs")
    expected_format = {
        EurostatUnemploymentArtifactRole.SEARCH_INDEX: OfficialSourceFormat.ATOM,
        EurostatUnemploymentArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
        EurostatUnemploymentArtifactRole.RELEASE_HTML: OfficialSourceFormat.HTML,
        EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
        EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
    }[role]
    if snapshot.request.source_format is not expected_format:
        raise ValueError(
            "Eurostat unemployment snapshot format differs from artifact role"
        )
    if snapshot.status_code != 200:
        raise ValueError("Eurostat unemployment snapshot status differs")
    if role is EurostatUnemploymentArtifactRole.SEARCH_INDEX:
        if (
            snapshot.request.page_number is None
            or snapshot.request.uri != _search_uri(snapshot.request.page_number)
        ):
            raise ValueError(
                "Eurostat unemployment search request identity differs"
            )
        try:
            root = safe_xml_fromstring(snapshot.content)
        except (DefusedXmlException, ParseError) as exc:
            raise ValueError(
                "Eurostat unemployment search artifact is invalid XML"
            ) from exc
        if root.tag != f"{{{_ATOM_NAMESPACE}}}feed":
            raise ValueError(
                "Eurostat unemployment search artifact is not Atom"
            )
    elif role is EurostatUnemploymentArtifactRole.CURRENT_DATASET:
        if (
            snapshot.request.page_number is not None
            or snapshot.request.uri != EUROSTAT_UNEMPLOYMENT_DATASET_URI
        ):
            raise ValueError(
                "Eurostat unemployment dataset request identity differs"
            )
        if not snapshot.content.lstrip().startswith(b"{"):
            raise ValueError(
                "Eurostat unemployment dataset artifact is not JSON"
            )
    elif snapshot.request.page_number is not None:
        raise ValueError(
            "Eurostat unemployment release request has a page number"
        )
    elif role in {
        EurostatUnemploymentArtifactRole.RELEASE_HTML,
        EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML,
    }:
        if b"<html" not in snapshot.content[:16_384].lower():
            raise ValueError("Eurostat unemployment HTML signature differs")
    elif not snapshot.content.startswith(b"%PDF-"):
        raise ValueError("Eurostat unemployment PDF signature differs")
    if role in {
        EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML,
        EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_PDF,
    } and (
        product_code is None
        or _product_code(snapshot.request.uri) != _product_code(product_code)
    ):
        raise ValueError("Eurostat unemployment document product code differs")
    return EurostatUnemploymentArtifactV1(
        role=role,
        request_uri=snapshot.request.uri,
        resolved_uri=snapshot.resolved_uri,
        source_format=expected_format,
        content_length=len(snapshot.content),
        content_sha256=snapshot.content_sha256,
        page_number=(
            snapshot.request.page_number
            if role is EurostatUnemploymentArtifactRole.SEARCH_INDEX
            else None
        ),
        product_code=product_code,
    )


def _atom_text(entry: Element, tag: str) -> str:
    node = entry.find(f"{{{_ATOM_NAMESPACE}}}{tag}")
    if node is None or node.text is None:
        raise ValueError(f"Eurostat unemployment Atom entry omits {tag}")
    return html.unescape(node.text)


def _atom_link(entry: Element) -> str:
    links = tuple(
        item
        for item in entry.findall(f"{{{_ATOM_NAMESPACE}}}link")
        if item.attrib.get("rel") == "alternate"
    )
    if len(links) != 1 or "href" not in links[0].attrib:
        raise ValueError(
            "Eurostat unemployment Atom entry alternate link differs"
        )
    return _https_uri(links[0].attrib["href"], "source_uri")


def parse_eurostat_unemployment_inventory(
    snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatUnemploymentInventoryV1:
    """Parse and prove the complete bounded unemployment search selection."""
    as_of = _iso_date(as_of_date, "as_of_date")
    values = tuple(snapshots)
    if len(values) != EUROSTAT_UNEMPLOYMENT_SEARCH_PAGE_COUNT:
        raise ValueError(
            "Eurostat unemployment inventory snapshot count differs"
        )
    if [item.request.page_number for item in values] != list(
        range(1, EUROSTAT_UNEMPLOYMENT_SEARCH_PAGE_COUNT + 1)
    ):
        raise ValueError(
            "Eurostat unemployment search snapshots are not contiguous"
        )
    artifacts: list[EurostatUnemploymentArtifactV1] = []
    raw_entries: list[tuple[int, str, str, str, str, str]] = []
    for snapshot in values:
        artifact = _artifact_from_snapshot(
            snapshot, EurostatUnemploymentArtifactRole.SEARCH_INDEX
        )
        artifacts.append(artifact)
        try:
            root = safe_xml_fromstring(snapshot.content)
        except (DefusedXmlException, ParseError) as exc:
            raise ValueError(
                "Eurostat unemployment search page is invalid XML"
            ) from exc
        entries = tuple(root.findall(f"{{{_ATOM_NAMESPACE}}}entry"))
        expected_count = 83 if artifact.page_number == 4 else 100
        if len(entries) != expected_count:
            raise ValueError("Eurostat unemployment search page size differs")
        for entry in entries:
            page_number = cast(int, artifact.page_number)
            raw_entries.append(
                (
                    page_number,
                    _atom_text(entry, "title"),
                    _atom_link(entry),
                    _atom_text(entry, "published"),
                    _atom_text(entry, "updated"),
                    _atom_text(entry, "summary"),
                )
            )
    if len(raw_entries) != EUROSTAT_UNEMPLOYMENT_SEARCH_RESULT_COUNT:
        raise ValueError("Eurostat unemployment search result count differs")
    uris = [item[2] for item in raw_entries]
    if len(uris) != len(set(uris)):
        raise ValueError("Eurostat unemployment search inventory repeats a URI")
    title_match_count = sum(
        re.search(r"\bunemployment\b", title, re.IGNORECASE) is not None
        for _, title, _, _, _, _ in raw_entries
    )
    prefiltered = [
        item for item in raw_entries if _inventory_prefilter(item[1])
    ]
    selected: list[EurostatUnemploymentInventoryEntryV1] = []
    exclusions: list[EurostatUnemploymentIndexExclusionV1] = []
    for page, title, uri, published, updated, summary in raw_entries:
        product_code = _product_code(uri)
        release_date = _product_release_date(product_code)
        if release_date > as_of:
            continue
        if not _inventory_selected(title, product_code):
            exclusions.append(
                EurostatUnemploymentIndexExclusionV1(
                    product_code=product_code,
                    release_date=release_date,
                    source_title=title,
                    source_uri=uri,
                    reason=_exclusion_reason(title),
                )
            )
            continue
        selected.append(
            EurostatUnemploymentInventoryEntryV1(
                product_code=product_code,
                release_date=release_date,
                source_title=title,
                source_uri=uri,
                atom_published_at=published,
                atom_updated_at=updated,
                summary=summary,
                page_number=page,
            )
        )
    selected.sort(key=lambda item: (item.release_date, item.product_code))
    exclusions.sort(key=lambda item: (item.release_date, item.product_code))
    return EurostatUnemploymentInventoryV1(
        artifacts=tuple(artifacts),
        entries=tuple(selected),
        exclusions=tuple(exclusions),
        query_result_count=len(raw_entries),
        title_match_count=title_match_count,
        prefilter_count=len(prefiltered),
    )


def _dimension_codes(
    dimension: Mapping[str, Any], name: str
) -> tuple[str, ...]:
    category = _mapping(dimension.get("category"), f"{name}.category")
    indexes = _mapping(category.get("index"), f"{name}.category.index")
    pairs: list[tuple[int, str]] = []
    for code, position in indexes.items():
        if isinstance(position, bool) or not isinstance(position, int):
            raise TypeError(f"{name} position must be an integer")
        pairs.append((position, str(code)))
    pairs.sort()
    if [item[0] for item in pairs] != list(range(len(pairs))):
        raise ValueError(f"{name} positions are not contiguous")
    return tuple(item[1] for item in pairs)


def parse_eurostat_unemployment_dataset(
    snapshot: OfficialRawSnapshotV1,
) -> EurostatUnemploymentDatasetV1:
    """Decode the exact current-revised unemployment JSON-stat cross-check."""
    artifact = _artifact_from_snapshot(
        snapshot, EurostatUnemploymentArtifactRole.CURRENT_DATASET
    )
    try:
        payload = _mapping(json.loads(snapshot.content), "dataset")
        lexical_payload = _mapping(
            json.loads(snapshot.content, parse_float=str), "lexical dataset"
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "Eurostat unemployment dataset is invalid JSON"
        ) from exc
    identifiers = [str(item) for item in _sequence(payload.get("id"), "id")]
    expected_ids = ["freq", "s_adj", "age", "unit", "sex", "geo", "time"]
    if identifiers != expected_ids:
        raise ValueError("Eurostat unemployment dataset dimensions differ")
    sizes = tuple(_sequence(payload.get("size"), "size"))
    if any(
        isinstance(item, bool) or not isinstance(item, int) for item in sizes
    ):
        raise TypeError(
            "Eurostat unemployment dimension sizes must be integers"
        )
    dimension = _mapping(payload.get("dimension"), "dimension")
    expected_codes = {
        "freq": ("M",),
        "s_adj": ("SA",),
        "age": ("TOTAL",),
        "unit": ("PC_ACT",),
        "sex": ("T",),
        "geo": _ECONOMIES,
    }
    observed_codes = {
        name: _dimension_codes(_mapping(dimension[name], name), name)
        for name in expected_codes
    }
    if observed_codes != expected_codes:
        raise ValueError(
            "Eurostat unemployment dataset dimensions or codes differ"
        )
    time_codes = _dimension_codes(_mapping(dimension["time"], "time"), "time")
    if not time_codes or any(
        _MONTH_RE.fullmatch(item) is None for item in time_codes
    ):
        raise ValueError("Eurostat unemployment dataset time codes differ")
    if time_codes != _month_range(time_codes[0], time_codes[-1]):
        raise ValueError(
            "Eurostat unemployment dataset time codes are not contiguous"
        )
    expected_sizes = (1, 1, 1, 1, 1, len(_ECONOMIES), len(time_codes))
    if tuple(cast(Sequence[int], sizes)) != expected_sizes:
        raise ValueError("Eurostat unemployment dataset sizes differ")
    values = _mapping(payload.get("value"), "value")
    lexical_values = _mapping(lexical_payload.get("value"), "lexical value")
    statuses = _mapping(payload.get("status", {}), "status")
    if set(values) != set(lexical_values):
        raise ValueError(
            "Eurostat unemployment lexical and numeric values differ"
        )
    if not set(statuses) <= set(values):
        raise ValueError("Eurostat unemployment status has no observation")
    observations: list[EurostatUnemploymentObservationV1] = []
    time_count = len(time_codes)
    geo_count = len(_ECONOMIES)
    maximum = geo_count * time_count
    for raw_index, raw_value in values.items():
        try:
            flat_index = int(str(raw_index))
        except ValueError as exc:
            raise ValueError(
                "Eurostat unemployment sparse index is invalid"
            ) from exc
        if not 0 <= flat_index < maximum:
            raise ValueError(
                "Eurostat unemployment sparse index is outside its cube"
            )
        geo_index, time_index = divmod(flat_index, time_count)
        lexical = str(lexical_values[raw_index])
        observations.append(
            EurostatUnemploymentObservationV1(
                economy_code=_ECONOMIES[geo_index],
                reference_period=time_codes[time_index],
                measure=EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE,
                value_lexical=lexical,
                value=float(cast(float, raw_value)),
                status=cast(str | None, statuses.get(raw_index)),
                flat_index=flat_index,
            )
        )
    observations.sort(
        key=lambda item: (
            _ECONOMY_ORDER[item.economy_code],
            item.reference_period,
        )
    )
    return EurostatUnemploymentDatasetV1(
        label=str(payload.get("label", "")),
        updated_at=str(payload.get("updated", "")),
        time_start=time_codes[0],
        time_end=time_codes[-1],
        artifact=artifact,
        observations=tuple(observations),
    )


class EurostatUnemploymentMovement(str, Enum):
    """Direction wording attached to the source-authored headline rate."""

    REPORTED = "reported"
    UP = "up"
    DOWN = "down"
    STABLE = "stable"

    @classmethod
    def from_value(
        cls, value: str | EurostatUnemploymentMovement
    ) -> EurostatUnemploymentMovement:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat unemployment movement"
            ) from exc


@dataclass(frozen=True, slots=True)
class EurostatUnemploymentReleaseValueV1:
    """Source-era euro-area unemployment rate in a release headline."""

    economy_code: str
    reference_period: str
    measure: EurostatUnemploymentMeasure
    movement: EurostatUnemploymentMovement
    value_lexical: str
    value: float
    evidence_id: str
    release_value_id: str = ""
    schema_version: str = EUROSTAT_UNEMPLOYMENT_RELEASE_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_UNEMPLOYMENT_RELEASE_VALUE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat unemployment release-value schema"
            )
        if self.economy_code != "EA":
            raise ValueError(
                "Eurostat unemployment release value must retain source-era EA scope"
            )
        reference = _month(self.reference_period, "reference_period")
        measure = EurostatUnemploymentMeasure.from_value(self.measure)
        if measure is not EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE:
            raise ValueError("Eurostat unemployment headline measure differs")
        movement = EurostatUnemploymentMovement.from_value(self.movement)
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None or lexical.startswith("-"):
            raise ValueError("Eurostat unemployment headline rate is invalid")
        numeric = float(lexical)
        if self.value != numeric or not math.isfinite(self.value):
            raise ValueError(
                "Eurostat unemployment headline numeric value differs"
            )
        if not 0.0 <= self.value <= 100.0:
            raise ValueError(
                "Eurostat unemployment headline value is outside its bound"
            )
        evidence_id = _required_text(self.evidence_id, "evidence_id")
        if not evidence_id.startswith(
            "eurostat-unemployment-inventory-entry:sha256:"
        ):
            raise ValueError("Eurostat unemployment headline evidence differs")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "movement", movement)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "evidence_id", evidence_id)
        expected = _stable_id(
            "eurostat-unemployment-release-value", self.identity_payload()
        )
        if self.release_value_id and self.release_value_id != expected:
            raise ValueError(
                "Eurostat unemployment release-value identity differs"
            )
        object.__setattr__(self, "release_value_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "reference_period": self.reference_period,
            "measure": self.measure.value,
            "movement": self.movement.value,
            "value_lexical": self.value_lexical,
            "value": self.value,
            "evidence_id": self.evidence_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "release_value_id": self.release_value_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatUnemploymentReleaseValueV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatUnemploymentMeasure.from_value(
                str(data.get("measure", ""))
            ),
            movement=EurostatUnemploymentMovement.from_value(
                str(data.get("movement", ""))
            ),
            value_lexical=str(data.get("value_lexical", "")),
            value=float(cast(float, data.get("value"))),
            evidence_id=str(data.get("evidence_id", "")),
            release_value_id=str(data.get("release_value_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatUnemploymentPublishedValueV1:
    """One source-era value read from a release publication table."""

    economy_code: str
    reference_period: str
    measure: EurostatUnemploymentMeasure
    value_lexical: str
    value: float
    evidence_artifact_sha256: str
    evidence_locator: str
    published_value_id: str = ""
    schema_version: str = EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat unemployment published-value schema"
            )
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _RELEASE_ECONOMIES:
            raise ValueError(
                "Eurostat unemployment published-value economy differs"
            )
        reference = _month(self.reference_period, "reference_period")
        measure = EurostatUnemploymentMeasure.from_value(self.measure)
        if measure is not EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE:
            raise ValueError("Eurostat unemployment published measure differs")
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _SIGNED_NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError(
                "Eurostat unemployment published-value lexical is invalid"
            )
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError(
                "Eurostat unemployment published-value numeric differs"
            )
        if not 0.0 <= numeric <= 100.0:
            raise ValueError(
                "Eurostat unemployment published value is outside its bound"
            )
        digest = _sha256(
            self.evidence_artifact_sha256, "evidence_artifact_sha256"
        )
        locator = _required_text(
            self.evidence_locator, "evidence_locator", maximum=1_024
        )
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "evidence_artifact_sha256", digest)
        object.__setattr__(self, "evidence_locator", locator)
        expected = _stable_id(
            "eurostat-unemployment-published-value", self.identity_payload()
        )
        if self.published_value_id and self.published_value_id != expected:
            raise ValueError(
                "Eurostat unemployment published-value identity differs"
            )
        object.__setattr__(self, "published_value_id", expected)

    @property
    def key(self) -> tuple[str, str, str]:
        return self.reference_period, self.economy_code, self.measure.value

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "reference_period": self.reference_period,
            "measure": self.measure.value,
            "value_lexical": self.value_lexical,
            "value": self.value,
            "evidence_artifact_sha256": self.evidence_artifact_sha256,
            "evidence_locator": self.evidence_locator,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "published_value_id": self.published_value_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatUnemploymentPublishedValueV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatUnemploymentMeasure.from_value(
                str(data.get("measure", ""))
            ),
            value_lexical=str(data.get("value_lexical", "")),
            value=float(cast(float, data.get("value"))),
            evidence_artifact_sha256=str(
                data.get("evidence_artifact_sha256", "")
            ),
            evidence_locator=str(data.get("evidence_locator", "")),
            published_value_id=str(data.get("published_value_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _month_end(value: str) -> date:
    token = _month(value, "reference_period")
    year, month = (int(item) for item in token.split("-"))
    return date(year, month, monthrange(year, month)[1])


def _ea_composition_for_period(reference_period: str) -> str:
    reference = _month(reference_period, "reference_period")
    boundaries = (
        ("2006-12", "EA12"),
        ("2007-12", "EA13"),
        ("2008-12", "EA15"),
        ("2010-12", "EA16"),
        ("2013-12", "EA17"),
        ("2014-12", "EA18"),
        ("2022-12", "EA19"),
        ("2025-12", "EA20"),
        ("9999-12", "EA21"),
    )
    return next(
        label for boundary, label in boundaries if reference <= boundary
    )


def _release_lag(release_date: str, reference_period: str) -> int:
    released = date.fromisoformat(_iso_date(release_date, "release_date"))
    reference = _month(reference_period, "reference_period")
    lag = (released - _month_end(reference)).days
    if not 20 <= lag <= 70:
        raise ValueError(
            "Eurostat unemployment release lag is outside the qualified window"
        )
    return lag


_HEADLINE_VALUE_RE = re.compile(
    r"\bunemployment(?:\s+rate)?\s+"
    r"(?:(?P<movement>up|down|stable|unchanged)\s+)?"
    r"(?:to|at)\s+(?P<value>\d+(?:\.\d+)?)\s*%",
    re.IGNORECASE,
)


def _headline_value(
    entry: EurostatUnemploymentInventoryEntryV1,
    reference_period: str,
) -> EurostatUnemploymentReleaseValueV1:
    match = _HEADLINE_VALUE_RE.search(entry.source_title)
    if match is None:
        raise ValueError("Eurostat unemployment release headline has no rate")
    movement_token = (match.group("movement") or "reported").casefold()
    movement = (
        EurostatUnemploymentMovement.STABLE
        if movement_token == "unchanged"
        else EurostatUnemploymentMovement.from_value(movement_token)
    )
    lexical = match.group("value")
    numeric = float(lexical)
    return EurostatUnemploymentReleaseValueV1(
        economy_code="EA",
        reference_period=reference_period,
        measure=EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE,
        movement=movement,
        value_lexical=lexical,
        value=numeric,
        evidence_id=entry.entry_id,
    )


class _ReleaseLandingParser(HTMLParser):
    """Collect source-authored landing metadata and document links."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.og_titles: list[str] = []
        self.published_at_values: list[str] = []
        self.text: list[str] = []
        self.hrefs: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = {key.casefold(): value for key, value in attrs}
        if tag.casefold() == "meta" and (
            (values.get("property") or "").casefold() == "og:title"
        ):
            content = values.get("content")
            if content:
                self.og_titles.append(content)
        if tag.casefold() == "meta" and (
            (values.get("property") or "").casefold()
            in {"article:published_time", "og:book:release_date"}
        ):
            content = values.get("content")
            if content:
                self.published_at_values.append(content)
        if tag.casefold() == "a":
            href = values.get("href")
            title = values.get("title") or ""
            if href and (
                "download publication" in title.casefold()
                or "/eurostat/documents/" in href.casefold()
            ):
                self.hrefs.append(href)

    def handle_data(self, data: str) -> None:
        value = _SPACE_RE.sub(" ", data).strip()
        if value:
            self.text.append(value)


_MONTHS: Final = {
    name.casefold(): number
    for number, name in enumerate(
        (
            "",
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        )
    )
    if name
}
_LANDING_DATE_RE = re.compile(
    r"\bRelease date:\s*(?P<day>\d{1,2})\s+"
    r"(?P<month>[A-Za-z]+)\s+(?P<year>\d{4})\b",
    re.IGNORECASE,
)
_LANDING_JSON_DATE_RE = re.compile(
    r'"datePublished"\s*:\s*"(?P<value>[^"\\]+)"', re.IGNORECASE
)


def _parse_release_landing(
    snapshot: OfficialRawSnapshotV1,
    entry: EurostatUnemploymentInventoryEntryV1,
) -> tuple[str, str, str | None, tuple[str, ...]]:
    try:
        content = snapshot.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "Eurostat unemployment release landing is not UTF-8"
        ) from exc
    parser = _ReleaseLandingParser()
    parser.feed(content)
    parser.close()
    titles = tuple(
        dict.fromkeys(
            _required_text(item, "landing_title") for item in parser.og_titles
        )
    )
    if titles != (entry.source_title,):
        raise ValueError("Eurostat unemployment Atom and landing titles differ")
    text = " ".join(parser.text)
    page_dates: set[str] = set()
    for date_match in _LANDING_DATE_RE.finditer(text):
        month = _MONTHS.get(date_match.group("month").casefold())
        if month is None:
            raise ValueError(
                "Eurostat unemployment landing month is unsupported"
            )
        try:
            page_dates.add(
                date(
                    int(date_match.group("year")),
                    month,
                    int(date_match.group("day")),
                ).isoformat()
            )
        except ValueError as exc:
            raise ValueError(
                "Eurostat unemployment landing date is invalid"
            ) from exc
    published_values = tuple(
        dict.fromkeys(
            _iso_datetime(item, "landing_published_at")
            for item in (
                *parser.published_at_values,
                *(
                    match.group("value")
                    for match in _LANDING_JSON_DATE_RE.finditer(content)
                ),
            )
        )
    )
    if len(published_values) > 1:
        raise ValueError(
            "Eurostat unemployment release landing has no unique publication time"
        )
    published_at = published_values[0] if published_values else None
    if published_at is not None:
        page_dates.add(
            parse_source_iso_datetime(published_at.replace("Z", "+00:00"))
            .date()
            .isoformat()
        )
    if len(page_dates) != 1:
        raise ValueError(
            "Eurostat unemployment release landing has no unique release date"
        )
    page_date = next(iter(page_dates))
    document_uris: set[str] = set()
    for href in parser.hrefs:
        uri = _https_uri(urljoin(snapshot.resolved_uri, href), "document_uri")
        try:
            linked_code = _product_code(uri)
        except ValueError:
            continue
        if (
            linked_code == entry.product_code
            and f"{entry.product_code}-en." in urlsplit(uri).path.casefold()
        ):
            document_uris.add(uri)
    if len(document_uris) > 1:
        raise ValueError(
            "Eurostat unemployment landing exposes multiple English release documents"
        )
    return titles[0], page_date, published_at, tuple(sorted(document_uris))


class _VisibleTextParser(HTMLParser):
    """Extract visible HTML text while excluding non-visible elements."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.values: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag.casefold() in {"script", "style", "template", "noscript"}:
            self._ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if (
            tag.casefold() in {"script", "style", "template", "noscript"}
            and self._ignored_depth
        ):
            self._ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._ignored_depth:
            return
        value = _SPACE_RE.sub(" ", data).strip()
        if value:
            self.values.append(value)


def _release_source_text(snapshot: OfficialRawSnapshotV1) -> str:
    if snapshot.request.source_format is OfficialSourceFormat.HTML:
        try:
            source = snapshot.content.decode("utf-8")
        except UnicodeDecodeError:
            source = snapshot.content.decode("cp1252")
        parser = _VisibleTextParser()
        parser.feed(source)
        parser.close()
        text = " ".join(parser.values)
    elif snapshot.request.source_format is OfficialSourceFormat.PDF:
        try:
            reader = PdfReader(BytesIO(snapshot.content), strict=False)
            page_text: list[str] = []
            failed_pages: list[int] = []
            for page_number, page in enumerate(reader.pages, start=1):
                try:
                    page_text.append(page.extract_text() or "")
                except PdfReadError:
                    failed_pages.append(page_number)
            if len(failed_pages) > 1 or not any(
                item.strip() for item in page_text
            ):
                raise ValueError(
                    "Eurostat unemployment release PDF has too many unreadable pages"
                )
            text = " ".join(page_text)
        except (PdfReadError, ValueError) as exc:
            raise ValueError(
                "Eurostat unemployment release PDF text extraction failed"
            ) from exc
    else:
        raise ValueError(
            "Eurostat unemployment release value source format differs"
        )
    normalized = _SPACE_RE.sub(" ", text.replace("\u2212", "-")).strip()
    if not normalized:
        raise ValueError(
            "Eurostat unemployment release value source has no text"
        )
    return normalized


_REFERENCE_MONTH_RE = re.compile(
    r"\b(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(?P<year>20\d{2})\b",
    re.IGNORECASE,
)


def _reference_period_from_publication(
    entry: EurostatUnemploymentInventoryEntryV1,
    landing_snapshot: OfficialRawSnapshotV1,
    document_snapshot: OfficialRawSnapshotV1 | None,
) -> tuple[str, int]:
    snapshots = (
        (landing_snapshot, document_snapshot)
        if document_snapshot is not None
        else (landing_snapshot,)
    )
    source_texts = chain(
        (entry.summary,),
        (_release_source_text(snapshot) for snapshot in snapshots),
    )
    for text in source_texts:
        for match in _REFERENCE_MONTH_RE.finditer(text):
            month = _MONTHS[match.group("month").casefold()]
            reference_period = f"{match.group('year')}-{month:02d}"
            try:
                lag = _release_lag(entry.release_date, reference_period)
            except ValueError:
                continue
            return reference_period, lag
    raise ValueError(
        "Eurostat unemployment publication omits a qualified reference month for "
        f"{entry.product_code}"
    )


def _composition_from_publication(
    reference_period: str,
    landing_snapshot: OfficialRawSnapshotV1,
    document_snapshot: OfficialRawSnapshotV1 | None,
) -> tuple[str, bool]:
    expected = _ea_composition_for_period(reference_period)
    labels: set[str] = set()
    for snapshot in (landing_snapshot, document_snapshot):
        if snapshot is None:
            continue
        labels.update(
            "EA"
            + cast(
                str,
                match.group("parenthesized")
                or match.group("plain")
                or match.group("compact"),
            )
            for match in _EA_COMPOSITION_RE.finditer(
                _release_source_text(snapshot)
            )
        )
    if labels and expected not in labels:
        raise ValueError(
            "Eurostat unemployment source-stated EA composition differs: "
            f"expected={expected}, observed={tuple(sorted(labels))}"
        )
    return expected, expected in labels


_TOTALS_TABLE_START_RE = re.compile(
    r"\bSeasonally adjusted unemployment(?:,|\s)+totals\b",
    re.IGNORECASE,
)
_LEGACY_TOTALS_TABLE_START_RE = re.compile(
    r"\bSeasonally adjusted unemployment rates?\s*\(%\)\s+"
    r"total males and females\b",
    re.IGNORECASE,
)
_LEGACY_TOTALS_TABLE_END_RE = re.compile(
    r"(?:\bMALES\s+EU\d+\s+Euro(?:[ -]\s*)(?:area|zone)\b|"
    r"\bSEASONALLY ADJUSTED UNEMPLOYMENT RATES?\s*\(%\)\s+MALES\b)",
    re.IGNORECASE,
)
_LEGACY_TOTALS_HEADER_RE = re.compile(
    r"\bEU\d+\s+Euro(?:[ -]\s*)(?:area|zone)\s+"
    r"(?:B\s+DK\s+D\s+(?:GR|EL)\s+E\s+F|"
    r"BE\s+DK\s+DE\s+(?:GR|EL)\s+ES\s+FR)\b",
    re.IGNORECASE,
)
_LEGACY_TABLE_PERIOD_RE = re.compile(
    r"\b(?P<year>20\d{2})[.-](?P<month>0[1-9]|1[0-2])\b"
)
_TOTALS_TABLE_END_RE = re.compile(
    r"\bSeasonally adjusted (?:youth|unemployment rates?\s*\(%\),\s*by sex)\b",
    re.IGNORECASE,
)
_TABLE_MONTH_RE = re.compile(
    r"\b(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
    r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|"
    r"Nov(?:ember)?|Dec(?:ember)?)"
    r"[- ](?P<year>\d{2}|20\d{2})\b",
    re.IGNORECASE,
)
_TABLE_BARE_MONTH_RE = re.compile(
    r"\b(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|"
    r"Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|"
    r"Nov(?:ember)?|Dec(?:ember)?)\b",
    re.IGNORECASE,
)
_TABLE_MONTH_NUMBERS: Final = {
    month: number
    for number, month in enumerate(
        (
            "",
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        )
    )
    if month
}
_TABLE_RATE_TOKEN: Final = r"(?:\d+(?:\.\d+)?|:)"
_PDF_BROKEN_DECIMAL_RE = re.compile(r"(?<=\d)\.\s+(?=\d(?:\s|$))")
_PDF_BROKEN_YEAR_AFTER_TWO_RE = re.compile(r"\b2\s+(?P<suffix>0\d{2})\b")
_PDF_BROKEN_YEAR_AFTER_TWENTY_RE = re.compile(r"\b20\s+(?P<suffix>\d{2})\b")
_PDF_BROKEN_MONTH_RE = re.compile(
    r"\b(?P<prefix>Ma|Ap)\s+r(?=\s+(?:\d{2}|20\d{2})\b)", re.IGNORECASE
)
_TABLE_ROW_LABELS: Final[Mapping[str, tuple[str, ...]]] = {
    "EA": ("Euro area", "Euro-zone"),
    "DE": ("Germany", "DE", "D"),
    "FR": ("France", "FR", "F"),
}
_EA_COMPOSITION_NUMBER_PATTERN: Final = r"(?:12|13|15|16|17|18|19|20|21)"
_EA_COMPOSITION_RE = re.compile(
    r"(?:\bEuro[ -](?:area|zone)\s*(?:"
    rf"\d*\s*\((?:EA|EUR)[ -]?(?P<parenthesized>"
    rf"{_EA_COMPOSITION_NUMBER_PATTERN})\)|"
    rf"(?P<plain>{_EA_COMPOSITION_NUMBER_PATTERN})"
    r"(?=\s+\d+(?:\.\d+)?))|"
    rf"\bEA(?P<compact>{_EA_COMPOSITION_NUMBER_PATTERN})"
    r"(?:[1-9]\d?)?(?=\s+\d+(?:\.\d+)?))",
    re.IGNORECASE,
)
_EA_TABLE_ROW_RE = re.compile(
    rf"\b(?:Euro[ -](?:area|zone)|"
    rf"EA{_EA_COMPOSITION_NUMBER_PATTERN}(?:[1-9]\d?)?)(?=\s)",
    re.IGNORECASE,
)


def _split_header_period_tokens(
    section: str, reference_period: str
) -> tuple[str, ...]:
    """Decode the five-column 2024--2025 split year/month header."""
    first_row = _EA_TABLE_ROW_RE.search(section)
    if first_row is None:
        return ()
    header = section[: first_row.start()]
    header_months = tuple(
        match.group("month")[:3].casefold()
        for match in _TABLE_BARE_MONTH_RE.finditer(header)
    )
    periods = (
        _month_shift(reference_period, -12),
        _month_shift(reference_period, -3),
        _month_shift(reference_period, -2),
        _month_shift(reference_period, -1),
        reference_period,
    )
    expected_months = tuple(
        next(
            month
            for month, number in _TABLE_MONTH_NUMBERS.items()
            if number == int(period[-2:])
        )
        for period in periods
    )
    expanded_rate_years = (
        periods[0][:4],
        *tuple(dict.fromkeys(period[:4] for period in periods[1:])),
    )
    compact_rate_years = tuple(dict.fromkeys(period[:4] for period in periods))
    expected_year_headers = {
        (*compact_rate_years, *compact_rate_years),
        (*expanded_rate_years, *expanded_rate_years),
    }
    header_years = tuple(re.findall(r"\b20\d{2}\b", header))
    if header_years not in expected_year_headers or header_months != (
        *expected_months,
        *expected_months,
    ):
        return ()
    return periods


def _legacy_wide_published_values(
    text: str,
    artifact: EurostatUnemploymentArtifactV1,
    reference_period: str,
) -> tuple[EurostatUnemploymentPublishedValueV1, ...]:
    """Parse the legacy 20-economy total-sex monthly matrix."""
    start_match = _LEGACY_TOTALS_TABLE_START_RE.search(text)
    if start_match is None:
        return ()
    remainder = text[start_match.end() :]
    first_period = _LEGACY_TABLE_PERIOD_RE.search(remainder)
    if first_period is None:
        return ()
    if (
        _LEGACY_TOTALS_HEADER_RE.search(remainder[: first_period.start()])
        is None
    ):
        return ()
    end_match = _LEGACY_TOTALS_TABLE_END_RE.search(remainder)
    section = remainder[: end_match.start()] if end_match else remainder
    row_pattern = re.compile(
        rf"(?P<period>20\d{{2}}[.-](?:0[1-9]|1[0-2]))\s+"
        rf"(?P<values>{_TABLE_RATE_TOKEN}(?:\s+{_TABLE_RATE_TOKEN}){{18}})"
    )
    minimum_period = _month_shift(reference_period, -12)
    economy_columns = {"EA": 1, "DE": 4, "FR": 7}
    results: list[EurostatUnemploymentPublishedValueV1] = []
    for row_match in row_pattern.finditer(section):
        period = row_match.group("period").replace(".", "-")
        if not minimum_period <= period <= reference_period:
            continue
        values = row_match.group("values").split()
        for economy, column in economy_columns.items():
            lexical = values[column]
            if lexical == ":":
                continue
            results.append(
                EurostatUnemploymentPublishedValueV1(
                    economy_code=economy,
                    reference_period=period,
                    measure=(EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE),
                    value_lexical=lexical,
                    value=float(lexical),
                    evidence_artifact_sha256=artifact.content_sha256,
                    evidence_locator=(
                        "seasonally adjusted unemployment rates; total males "
                        f"and females matrix; {economy} column; {period}"
                    ),
                )
            )
    if not any(
        item.economy_code == "EA" and item.reference_period == reference_period
        for item in results
    ):
        raise ValueError(
            "Eurostat unemployment legacy totals table omits current EA rate "
            f"for {artifact.product_code}: reference_period={reference_period}"
        )
    return tuple(results)


def _table_published_values(
    snapshot: OfficialRawSnapshotV1,
    artifact: EurostatUnemploymentArtifactV1,
    reference_period: str,
) -> tuple[EurostatUnemploymentPublishedValueV1, ...]:
    text = _release_source_text(snapshot)
    text = _PDF_BROKEN_YEAR_AFTER_TWO_RE.sub(r"2\g<suffix>", text)
    text = _PDF_BROKEN_YEAR_AFTER_TWENTY_RE.sub(r"20\g<suffix>", text)
    text = _PDF_BROKEN_MONTH_RE.sub(r"\g<prefix>r", text)
    text = _PDF_BROKEN_DECIMAL_RE.sub(".", text)
    legacy = _legacy_wide_published_values(text, artifact, reference_period)
    if legacy:
        return legacy
    modern_start_match = _TOTALS_TABLE_START_RE.search(text)
    legacy_start_match = _LEGACY_TOTALS_TABLE_START_RE.search(text)
    start_matches = tuple(
        match
        for match in (modern_start_match, legacy_start_match)
        if match is not None
    )
    if not start_matches:
        return ()
    start_match = min(start_matches, key=lambda match: match.start())
    legacy_row_table = start_match is legacy_start_match
    remainder = text[start_match.start() :]
    end_matches = tuple(
        match
        for match in (
            _TOTALS_TABLE_END_RE.search(remainder),
            _LEGACY_TOTALS_TABLE_END_RE.search(
                remainder, start_match.end() - start_match.start()
            ),
        )
        if match is not None
    )
    end_match = min(end_matches, key=lambda match: match.start(), default=None)
    section = (
        remainder[: end_match.start()] if end_match is not None else remainder
    )
    first_ea_row = _EA_TABLE_ROW_RE.search(section)
    header = section[: first_ea_row.start()] if first_ea_row else section
    period_tokens: list[str] = []
    header_periods: list[tuple[int, str]] = []
    for match in _TABLE_MONTH_RE.finditer(header):
        year_token = match.group("year")
        year = int(year_token) + (2000 if len(year_token) == 2 else 0)
        month = _TABLE_MONTH_NUMBERS[match.group("month")[:3].casefold()]
        header_periods.append((match.start(), f"{year:04d}-{month:02d}"))
    header_periods.extend(
        (
            match.start(),
            f"{match.group('year')}-{match.group('month')}",
        )
        for match in _LEGACY_TABLE_PERIOD_RE.finditer(header)
    )
    for _, token in sorted(header_periods):
        if token in period_tokens:
            break
        period_tokens.append(token)
    if not period_tokens:
        period_tokens.extend(
            _split_header_period_tokens(section, reference_period)
        )
    if not period_tokens or reference_period not in period_tokens:
        raise ValueError(
            "Eurostat unemployment totals-table header differs for "
            f"{artifact.product_code}: reference_period={reference_period}"
        )
    results: list[EurostatUnemploymentPublishedValueV1] = []
    for economy, labels in _TABLE_ROW_LABELS.items():
        if economy == "EA" and re.search(
            rf"\b(?:Euro[ -](?:area|zone)\s+"
            rf"{_EA_COMPOSITION_NUMBER_PATTERN}|"
            rf"EA{_EA_COMPOSITION_NUMBER_PATTERN})"
            rf"(?:[1-9]\d?)?\s+"
            rf"{_TABLE_RATE_TOKEN}",
            section,
            re.IGNORECASE,
        ):
            composition_number = _ea_composition_for_period(
                reference_period
            ).removeprefix("EA")
            labels = (
                f"Euro area {composition_number}",
                f"Euro-zone {composition_number}",
                f"EA{composition_number}",
            )
        alternatives = "|".join(
            re.escape(item) for item in sorted(labels, key=len, reverse=True)
        )
        attached_footnote = r"(?P<attached_footnote>[1-9]\d?)?"
        extra_legacy_token = 1 if legacy_row_table else 0
        row_pattern = re.compile(
            rf"(?<![\w-])(?P<label>{alternatives}){attached_footnote}"
            rf"(?![\w-])"
            rf"(?:\s*\((?:EA|EUR)[ -]?"
            rf"{_EA_COMPOSITION_NUMBER_PATTERN}\))?(?:\*+)?\s+"
            rf"(?P<values>{_TABLE_RATE_TOKEN}(?:\s+{_TABLE_RATE_TOKEN})"
            rf"{{{len(period_tokens) - 1},"
            rf"{len(period_tokens) - 1 + extra_legacy_token}}})",
            re.IGNORECASE,
        )
        row_matches = tuple(row_pattern.finditer(section))
        if len(row_matches) > 1:
            signatures = {
                (
                    match.group("label").casefold(),
                    tuple(match.group("values").split()),
                )
                for match in row_matches
            }
            if len(signatures) > 1:
                raise ValueError(
                    "Eurostat unemployment publication repeats a conflicting "
                    "totals-table row for "
                    f"{artifact.product_code} {economy}: "
                    f"matches={len(row_matches)}"
                )
        if not row_matches:
            continue
        row_match = row_matches[0]
        lexical_values = row_match.group("values").split()
        if len(lexical_values) == len(period_tokens) + 1:
            if re.fullmatch(r"[1-9]\d?", lexical_values[0]) is None:
                raise ValueError(
                    "Eurostat unemployment totals-table footnote differs for "
                    f"{artifact.product_code} {economy}"
                )
            lexical_values = lexical_values[1:]
        for period, lexical in zip(period_tokens, lexical_values, strict=True):
            if lexical == ":":
                continue
            results.append(
                EurostatUnemploymentPublishedValueV1(
                    economy_code=economy,
                    reference_period=period,
                    measure=EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE,
                    value_lexical=lexical,
                    value=float(lexical),
                    evidence_artifact_sha256=artifact.content_sha256,
                    evidence_locator=(
                        "seasonally adjusted unemployment totals table; "
                        f"{row_match.group('label')} row; {period}; rate percent"
                    ),
                )
            )
    if not any(
        item.economy_code == "EA" and item.reference_period == reference_period
        for item in results
    ):
        raise ValueError(
            "Eurostat unemployment totals table omits current EA rate for "
            f"{artifact.product_code}: reference_period={reference_period}"
        )
    return tuple(results)


def _published_values(
    landing_snapshot: OfficialRawSnapshotV1,
    landing_artifact: EurostatUnemploymentArtifactV1,
    document_snapshot: OfficialRawSnapshotV1 | None,
    document_artifact: EurostatUnemploymentArtifactV1 | None,
    reference_period: str,
    headline: EurostatUnemploymentReleaseValueV1,
) -> tuple[EurostatUnemploymentPublishedValueV1, ...]:
    parsed: tuple[EurostatUnemploymentPublishedValueV1, ...] = ()
    for snapshot, artifact in (
        (landing_snapshot, landing_artifact),
        (document_snapshot, document_artifact),
    ):
        if snapshot is None or artifact is None:
            continue
        parsed = _table_published_values(snapshot, artifact, reference_period)
        if parsed:
            break
    values = list(parsed)
    headline_match = next(
        (
            item
            for item in values
            if item.economy_code == "EA"
            and item.measure is EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE
            and item.reference_period == reference_period
        ),
        None,
    )
    if headline_match is None:
        values.append(
            EurostatUnemploymentPublishedValueV1(
                economy_code="EA",
                reference_period=reference_period,
                measure=EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE,
                value_lexical=headline.value_lexical,
                value=headline.value,
                evidence_artifact_sha256=landing_artifact.content_sha256,
                evidence_locator="source-authored release headline",
            )
        )
    elif headline_match.value != headline.value:
        raise ValueError(
            "Eurostat unemployment headline and publication table differ for "
            f"{landing_artifact.product_code}: headline={headline.value}, "
            f"table={headline_match.value}"
        )
    values.sort(
        key=lambda item: (
            item.reference_period,
            _RELEASE_ECONOMY_ORDER[item.economy_code],
        )
    )
    return tuple(values)


@dataclass(frozen=True, slots=True)
class EurostatUnemploymentReleaseV1:
    """One source-dated monthly unemployment release."""

    inventory_entry: EurostatUnemploymentInventoryEntryV1
    reference_period: str
    days_after_period_end: int
    euro_area_composition: str
    composition_source_stated: bool
    page_release_date: str
    page_release_date_offset_days: int
    published_at: str | None
    linked_document_uris: tuple[str, ...]
    artifact: EurostatUnemploymentArtifactV1
    document_artifact: EurostatUnemploymentArtifactV1 | None
    headline_value: EurostatUnemploymentReleaseValueV1
    published_values: tuple[EurostatUnemploymentPublishedValueV1, ...]
    release_id: str = ""
    schema_version: str = EUROSTAT_UNEMPLOYMENT_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_UNEMPLOYMENT_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat unemployment release schema")
        reference = _month(self.reference_period, "reference_period")
        expected_lag = _release_lag(
            self.inventory_entry.release_date, reference
        )
        if self.days_after_period_end != expected_lag:
            raise ValueError(
                "Eurostat unemployment release period or lag differs"
            )
        composition = _required_text(
            self.euro_area_composition, "euro_area_composition"
        )
        if composition != _ea_composition_for_period(reference):
            raise ValueError("Eurostat unemployment EA composition differs")
        if not isinstance(self.composition_source_stated, bool):
            raise TypeError("composition_source_stated must be boolean")
        page_date = _iso_date(self.page_release_date, "page_release_date")
        if isinstance(
            self.page_release_date_offset_days, bool
        ) or not isinstance(self.page_release_date_offset_days, int):
            raise TypeError("page_release_date_offset_days must be an integer")
        offset = (
            date.fromisoformat(page_date)
            - date.fromisoformat(self.inventory_entry.release_date)
        ).days
        expected_exception = (
            EUROSTAT_UNEMPLOYMENT_PAGE_DATE_OFFSET_EXCEPTIONS.get(
                self.inventory_entry.product_code
            )
        )
        if self.page_release_date_offset_days != offset or (
            (expected_exception is None and offset not in {-1, 0})
            or (expected_exception is not None and offset != expected_exception)
        ):
            raise ValueError(
                "Eurostat unemployment landing date offset differs for "
                f"{self.inventory_entry.product_code}: offset={offset}"
            )
        published_at = _optional_text(self.published_at, "published_at")
        if published_at is not None:
            published_at = _iso_datetime(published_at, "published_at")
            published_date = parse_source_iso_datetime(
                published_at.replace("Z", "+00:00")
            ).date()
            if published_date.isoformat() != page_date:
                raise ValueError(
                    "Eurostat unemployment publication timestamp date differs"
                )
        links = tuple(
            _https_uri(item, "linked_document_uri")
            for item in self.linked_document_uris
        )
        if links != tuple(sorted(set(links))):
            raise ValueError(
                "Eurostat unemployment release document links are not canonical"
            )
        if len(links) > 1:
            raise ValueError(
                "Eurostat unemployment release has multiple English documents"
            )
        if any(
            _product_code(item) != self.inventory_entry.product_code
            for item in links
        ):
            raise ValueError(
                "Eurostat unemployment release document product code differs"
            )
        if (
            self.artifact.role
            is not EurostatUnemploymentArtifactRole.RELEASE_HTML
        ):
            raise ValueError(
                "Eurostat unemployment release artifact role differs"
            )
        if (
            self.artifact.product_code != self.inventory_entry.product_code
            or self.artifact.request_uri != self.inventory_entry.source_uri
            or _product_code(self.artifact.resolved_uri)
            != self.inventory_entry.product_code
        ):
            raise ValueError(
                "Eurostat unemployment release artifact product code differs"
            )
        document = self.document_artifact
        if (document is None) != (not links):
            raise ValueError(
                "Eurostat unemployment release document artifact differs"
            )
        if document is not None and (
            document.role
            not in {
                EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML,
                EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_PDF,
            }
            or document.request_uri != links[0]
            or document.product_code != self.inventory_entry.product_code
            or _product_code(document.resolved_uri)
            != self.inventory_entry.product_code
        ):
            raise ValueError(
                "Eurostat unemployment release document identity differs"
            )
        value = self.headline_value
        if (
            value.reference_period != reference
            or value.evidence_id != self.inventory_entry.entry_id
        ):
            raise ValueError(
                "Eurostat unemployment release headline occurrence differs"
            )
        published_values = tuple(self.published_values)
        if not (
            1
            <= len(published_values)
            <= MAX_EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUES_PER_RELEASE
        ):
            raise ValueError(
                "Eurostat unemployment published-value count differs"
            )
        keys = [item.key for item in published_values]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "Eurostat unemployment release repeats a published value"
            )
        if list(published_values) != sorted(
            published_values,
            key=lambda item: (
                item.reference_period,
                _RELEASE_ECONOMY_ORDER[item.economy_code],
            ),
        ):
            raise ValueError(
                "Eurostat unemployment published values are not canonical"
            )
        evidence_digests = {self.artifact.content_sha256}
        if document is not None:
            evidence_digests.add(document.content_sha256)
        if any(
            item.reference_period > reference
            or item.reference_period < _month_shift(reference, -12)
            or item.evidence_artifact_sha256 not in evidence_digests
            for item in published_values
        ):
            raise ValueError(
                "Eurostat unemployment published-value evidence differs"
            )
        headline_published = next(
            (
                item
                for item in published_values
                if item.economy_code == "EA"
                and item.measure
                is EurostatUnemploymentMeasure.RATE_PERCENT_ACTIVE
                and item.reference_period == reference
            ),
            None,
        )
        if (
            headline_published is None
            or headline_published.value != value.value
        ):
            raise ValueError(
                "Eurostat unemployment headline published value differs"
            )
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "euro_area_composition", composition)
        object.__setattr__(self, "page_release_date", page_date)
        object.__setattr__(self, "published_at", published_at)
        object.__setattr__(self, "linked_document_uris", links)
        object.__setattr__(self, "published_values", published_values)
        expected = _stable_id(
            "eurostat-unemployment-release", self.identity_payload()
        )
        if self.release_id and self.release_id != expected:
            raise ValueError("Eurostat unemployment release identity differs")
        object.__setattr__(self, "release_id", expected)

    @property
    def occurrence_key(self) -> tuple[str, str]:
        return self.reference_period, self.inventory_entry.product_code

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "inventory_entry": self.inventory_entry.to_dict(),
            "reference_period": self.reference_period,
            "days_after_period_end": self.days_after_period_end,
            "euro_area_composition": self.euro_area_composition,
            "composition_source_stated": self.composition_source_stated,
            "page_release_date": self.page_release_date,
            "page_release_date_offset_days": self.page_release_date_offset_days,
            "published_at": self.published_at,
            "linked_document_uris": list(self.linked_document_uris),
            "artifact": self.artifact.to_dict(),
            "document_artifact": (
                self.document_artifact.to_dict()
                if self.document_artifact is not None
                else None
            ),
            "headline_value": self.headline_value.to_dict(),
            "published_values": [
                item.to_dict() for item in self.published_values
            ],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatUnemploymentReleaseV1:
        return cls(
            inventory_entry=EurostatUnemploymentInventoryEntryV1.from_dict(
                _mapping(data.get("inventory_entry"), "inventory_entry")
            ),
            reference_period=str(data.get("reference_period", "")),
            days_after_period_end=cast(int, data.get("days_after_period_end")),
            euro_area_composition=str(data.get("euro_area_composition", "")),
            composition_source_stated=cast(
                bool, data.get("composition_source_stated")
            ),
            page_release_date=str(data.get("page_release_date", "")),
            page_release_date_offset_days=cast(
                int, data.get("page_release_date_offset_days")
            ),
            published_at=_optional_text(
                data.get("published_at"), "published_at"
            ),
            linked_document_uris=tuple(
                str(item)
                for item in _sequence(
                    data.get("linked_document_uris"), "linked_document_uris"
                )
            ),
            artifact=EurostatUnemploymentArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            document_artifact=(
                EurostatUnemploymentArtifactV1.from_dict(
                    _mapping(data.get("document_artifact"), "document_artifact")
                )
                if data.get("document_artifact") is not None
                else None
            ),
            headline_value=EurostatUnemploymentReleaseValueV1.from_dict(
                _mapping(data.get("headline_value"), "headline_value")
            ),
            published_values=tuple(
                EurostatUnemploymentPublishedValueV1.from_dict(
                    _mapping(item, "published_value")
                )
                for item in _sequence(
                    data.get("published_values"), "published_values"
                )
            ),
            release_id=str(data.get("release_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _published_revision_counts(
    releases: Sequence[EurostatUnemploymentReleaseV1],
) -> tuple[int, int]:
    series: dict[tuple[str, str, str], list[float]] = {}
    for release in releases:
        for value in release.published_values:
            comparison_scope = (
                release.euro_area_composition
                if value.economy_code == "EA"
                else value.economy_code
            )
            key = (
                value.reference_period,
                comparison_scope,
                value.measure.value,
            )
            series.setdefault(key, []).append(value.value)
    comparison_count = sum(
        max(0, len(values) - 1) for values in series.values()
    )
    changed_count = sum(
        current != previous
        for values in series.values()
        for previous, current in pairwise(values)
    )
    return comparison_count, changed_count


def _current_dataset_comparison_counts(
    releases: Sequence[EurostatUnemploymentReleaseV1],
    dataset: EurostatUnemploymentDatasetV1,
) -> tuple[int, int]:
    current = {
        (item.economy_code, item.reference_period): item.value
        for item in dataset.observations
    }
    comparison_count = 0
    changed_count = 0
    for release in releases:
        for value in release.published_values:
            economy = value.economy_code
            if economy == "EA":
                if release.euro_area_composition != "EA21":
                    continue
                economy = "EA21"
            revised = current.get((economy, value.reference_period))
            if revised is None:
                continue
            comparison_count += 1
            changed_count += revised != value.value
    return comparison_count, changed_count


@dataclass(frozen=True, slots=True)
class EurostatUnemploymentArchiveManifestV1:
    """Compact, replayable receipt for the official unemployment corpus."""

    as_of_date: str
    registry_id: str
    source_id: str
    inventory: EurostatUnemploymentInventoryV1
    current_dataset: EurostatUnemploymentDatasetV1
    releases: tuple[EurostatUnemploymentReleaseV1, ...]
    searchable_release_gap_reference_periods: tuple[str, ...]
    composition_source_stated_count: int
    page_date_offset_count: int
    exact_publication_time_count: int
    release_document_count: int
    published_value_count: int
    revision_comparison_count: int
    changed_revision_count: int
    current_dataset_comparison_count: int
    changed_current_dataset_count: int
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    manifest_id: str = ""
    schema_version: str = EUROSTAT_UNEMPLOYMENT_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_UNEMPLOYMENT_MANIFEST_SCHEMA_VERSION:
            raise ValueError(
                "unsupported Eurostat unemployment manifest schema"
            )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        if as_of != EUROSTAT_UNEMPLOYMENT_AS_OF_DATE:
            raise ValueError("Eurostat unemployment archive boundary differs")
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError(
                "Eurostat unemployment registry identity is invalid"
            )
        if not source_id.startswith("official-source:sha256:"):
            raise ValueError("Eurostat unemployment source identity is invalid")
        releases = tuple(self.releases)
        if len(releases) != EUROSTAT_UNEMPLOYMENT_RELEASE_COUNT:
            raise ValueError(
                "Eurostat unemployment manifest release count differs"
            )
        if list(releases) != sorted(
            releases,
            key=lambda item: (
                item.inventory_entry.release_date,
                item.inventory_entry.product_code,
            ),
        ):
            raise ValueError("Eurostat unemployment releases are not canonical")
        if [item.inventory_entry for item in releases] != list(
            self.inventory.entries
        ):
            raise ValueError(
                "Eurostat unemployment release and inventory entries differ"
            )
        if any(
            item.headline_value
            != _headline_value(item.inventory_entry, item.reference_period)
            for item in releases
        ):
            raise ValueError(
                "Eurostat unemployment release headline evidence differs"
            )
        occurrences = [item.occurrence_key for item in releases]
        if len(occurrences) != len(set(occurrences)):
            raise ValueError(
                "Eurostat unemployment releases repeat an occurrence"
            )
        periods = [item.reference_period for item in releases]
        if len(periods) != len(set(periods)):
            raise ValueError("Eurostat unemployment releases repeat a month")
        if (
            min(periods) != EUROSTAT_UNEMPLOYMENT_FIRST_REFERENCE_PERIOD
            or max(periods) != EUROSTAT_UNEMPLOYMENT_LATEST_REFERENCE_PERIOD
        ):
            raise ValueError("Eurostat unemployment reference range differs")
        expected_release_periods = tuple(
            item
            for item in _month_range(
                EUROSTAT_UNEMPLOYMENT_FIRST_REFERENCE_PERIOD,
                EUROSTAT_UNEMPLOYMENT_LATEST_REFERENCE_PERIOD,
            )
            if item not in EUROSTAT_UNEMPLOYMENT_INTERNAL_GAP_REFERENCE_PERIODS
        )
        if tuple(periods) != expected_release_periods:
            raise ValueError(
                "Eurostat unemployment reference-month lineage differs"
            )
        if any(item.inventory_entry.release_date > as_of for item in releases):
            raise ValueError(
                "Eurostat unemployment release exceeds archive boundary"
            )
        if self.current_dataset.updated_at[:10] > as_of:
            raise ValueError(
                "Eurostat unemployment dataset exceeds archive boundary"
            )
        expected_gaps = (
            *_month_range(
                self.current_dataset.time_start,
                _month_shift(EUROSTAT_UNEMPLOYMENT_FIRST_REFERENCE_PERIOD, -1),
            ),
            *EUROSTAT_UNEMPLOYMENT_INTERNAL_GAP_REFERENCE_PERIODS,
        )
        gaps = tuple(
            _month(item, "searchable_release_gap_reference_period")
            for item in self.searchable_release_gap_reference_periods
        )
        if gaps != expected_gaps:
            raise ValueError(
                "Eurostat unemployment searchable-release gap inventory differs"
            )
        for name, maximum in (
            (
                "composition_source_stated_count",
                MAX_EUROSTAT_UNEMPLOYMENT_RELEASES,
            ),
            ("page_date_offset_count", MAX_EUROSTAT_UNEMPLOYMENT_RELEASES),
            (
                "exact_publication_time_count",
                MAX_EUROSTAT_UNEMPLOYMENT_RELEASES,
            ),
            ("release_document_count", MAX_EUROSTAT_UNEMPLOYMENT_RELEASES),
            (
                "published_value_count",
                MAX_EUROSTAT_UNEMPLOYMENT_RELEASES
                * MAX_EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "revision_comparison_count",
                MAX_EUROSTAT_UNEMPLOYMENT_RELEASES
                * MAX_EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "changed_revision_count",
                MAX_EUROSTAT_UNEMPLOYMENT_RELEASES
                * MAX_EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "current_dataset_comparison_count",
                MAX_EUROSTAT_UNEMPLOYMENT_RELEASES
                * MAX_EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "changed_current_dataset_count",
                MAX_EUROSTAT_UNEMPLOYMENT_RELEASES
                * MAX_EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUES_PER_RELEASE,
            ),
        ):
            _bounded_int(getattr(self, name), name, maximum)
        if self.composition_source_stated_count != sum(
            item.composition_source_stated for item in releases
        ):
            raise ValueError("Eurostat unemployment composition count differs")
        expected_offsets = sum(
            item.page_release_date_offset_days != 0 for item in releases
        )
        if self.page_date_offset_count != expected_offsets:
            raise ValueError(
                "Eurostat unemployment page-date offset count differs"
            )
        expected_exact_times = sum(
            item.published_at is not None for item in releases
        )
        if self.exact_publication_time_count != expected_exact_times:
            raise ValueError(
                "Eurostat unemployment exact publication-time count differs"
            )
        document_artifacts = tuple(
            item.document_artifact
            for item in releases
            if item.document_artifact is not None
        )
        if len(document_artifacts) != EUROSTAT_UNEMPLOYMENT_DOCUMENT_COUNT:
            raise ValueError(
                "Eurostat unemployment English document count differs"
            )
        if self.release_document_count != len(document_artifacts):
            raise ValueError(
                "Eurostat unemployment release-document count differs"
            )
        expected_value_count = sum(
            len(item.published_values) for item in releases
        )
        if self.published_value_count != expected_value_count:
            raise ValueError(
                "Eurostat unemployment published-value count differs"
            )
        revision_counts = _published_revision_counts(releases)
        if revision_counts != (
            self.revision_comparison_count,
            self.changed_revision_count,
        ):
            raise ValueError("Eurostat unemployment revision counts differ")
        dataset_counts = _current_dataset_comparison_counts(
            releases, self.current_dataset
        )
        if dataset_counts != (
            self.current_dataset_comparison_count,
            self.changed_current_dataset_count,
        ):
            raise ValueError("Eurostat unemployment dataset comparison differs")
        artifacts = (
            *self.inventory.artifacts,
            self.current_dataset.artifact,
            *(item.artifact for item in releases),
            *document_artifacts,
        )
        expected_artifact_count = len(artifacts)
        expected_unique_count = len({item.content_sha256 for item in artifacts})
        expected_bytes = sum(item.content_length for item in artifacts)
        counts = (
            _bounded_int(
                self.raw_artifact_count,
                "raw_artifact_count",
                MAX_EUROSTAT_UNEMPLOYMENT_ARTIFACTS,
            ),
            _bounded_int(
                self.unique_content_sha256_count,
                "unique_content_sha256_count",
                MAX_EUROSTAT_UNEMPLOYMENT_ARTIFACTS,
            ),
            _bounded_int(
                self.total_content_bytes,
                "total_content_bytes",
                MAX_EUROSTAT_UNEMPLOYMENT_TOTAL_BYTES,
            ),
        )
        if counts != (
            expected_artifact_count,
            expected_unique_count,
            expected_bytes,
        ):
            raise ValueError("Eurostat unemployment artifact totals differ")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "releases", releases)
        object.__setattr__(
            self, "searchable_release_gap_reference_periods", gaps
        )
        expected = _stable_id(
            "eurostat-unemployment-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("Eurostat unemployment manifest identity differs")
        object.__setattr__(self, "manifest_id", expected)

    @property
    def release_count(self) -> int:
        return len(self.releases)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "inventory": self.inventory.to_dict(),
            "current_dataset": self.current_dataset.to_dict(),
            "releases": [item.to_dict() for item in self.releases],
            "searchable_release_gap_reference_periods": list(
                self.searchable_release_gap_reference_periods
            ),
            "composition_source_stated_count": (
                self.composition_source_stated_count
            ),
            "page_date_offset_count": self.page_date_offset_count,
            "exact_publication_time_count": self.exact_publication_time_count,
            "release_document_count": self.release_document_count,
            "published_value_count": self.published_value_count,
            "revision_comparison_count": self.revision_comparison_count,
            "changed_revision_count": self.changed_revision_count,
            "current_dataset_comparison_count": (
                self.current_dataset_comparison_count
            ),
            "changed_current_dataset_count": (
                self.changed_current_dataset_count
            ),
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        encoded: str = canonical_contract_json(self.to_dict())
        return encoded

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatUnemploymentArchiveManifestV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            inventory=EurostatUnemploymentInventoryV1.from_dict(
                _mapping(data.get("inventory"), "inventory")
            ),
            current_dataset=EurostatUnemploymentDatasetV1.from_dict(
                _mapping(data.get("current_dataset"), "current_dataset")
            ),
            releases=tuple(
                EurostatUnemploymentReleaseV1.from_dict(
                    _mapping(item, "release")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            searchable_release_gap_reference_periods=tuple(
                str(item)
                for item in _sequence(
                    data.get("searchable_release_gap_reference_periods"),
                    "searchable_release_gap_reference_periods",
                )
            ),
            composition_source_stated_count=cast(
                int, data.get("composition_source_stated_count")
            ),
            page_date_offset_count=cast(
                int, data.get("page_date_offset_count")
            ),
            exact_publication_time_count=cast(
                int, data.get("exact_publication_time_count")
            ),
            release_document_count=cast(
                int, data.get("release_document_count")
            ),
            published_value_count=cast(int, data.get("published_value_count")),
            revision_comparison_count=cast(
                int, data.get("revision_comparison_count")
            ),
            changed_revision_count=cast(
                int, data.get("changed_revision_count")
            ),
            current_dataset_comparison_count=cast(
                int, data.get("current_dataset_comparison_count")
            ),
            changed_current_dataset_count=cast(
                int, data.get("changed_current_dataset_count")
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, value: str) -> EurostatUnemploymentArchiveManifestV1:
        try:
            data = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "Eurostat unemployment manifest is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_eurostat_unemployment_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    dataset_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatUnemploymentArchiveManifestV1:
    """Build the unemployment receipt from exact retained Eurostat bytes."""
    source = registry.source(EUROSTAT_UNEMPLOYMENT_SOURCE_KEY)
    if (
        source.parser_id != EUROSTAT_UNEMPLOYMENT_PARSER_ID
        or source.parser_version != EUROSTAT_UNEMPLOYMENT_PARSER_VERSION
    ):
        raise ValueError(
            "Eurostat unemployment registry parser identity differs"
        )
    search_values = tuple(search_snapshots)
    release_values = tuple(release_snapshots)
    document_values = tuple(document_snapshots)
    if any(
        snapshot.request.source_id != source.source_id
        for snapshot in (
            *search_values,
            dataset_snapshot,
            *release_values,
            *document_values,
        )
    ):
        raise ValueError(
            "Eurostat unemployment snapshot source identity differs"
        )
    inventory = parse_eurostat_unemployment_inventory(
        search_values, as_of_date=as_of_date
    )
    dataset = parse_eurostat_unemployment_dataset(dataset_snapshot)
    snapshots = release_values
    if len(snapshots) != EUROSTAT_UNEMPLOYMENT_RELEASE_COUNT:
        raise ValueError("Eurostat unemployment release snapshot count differs")
    by_code: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        code = _product_code(snapshot.request.uri)
        if code in by_code:
            raise ValueError(
                "Eurostat unemployment release corpus repeats a product code"
            )
        by_code[code] = snapshot
    if set(by_code) != {item.product_code for item in inventory.entries}:
        raise ValueError("Eurostat unemployment release corpus is incomplete")
    if len(document_values) != EUROSTAT_UNEMPLOYMENT_DOCUMENT_COUNT:
        raise ValueError(
            "Eurostat unemployment document snapshot count differs"
        )
    documents_by_uri: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in document_values:
        if snapshot.request.uri in documents_by_uri:
            raise ValueError(
                "Eurostat unemployment document corpus repeats a URI"
            )
        documents_by_uri[snapshot.request.uri] = snapshot
    used_document_uris: set[str] = set()
    parsed: list[
        tuple[
            EurostatUnemploymentInventoryEntryV1,
            EurostatUnemploymentArtifactV1,
            EurostatUnemploymentArtifactV1 | None,
            str,
            str | None,
            str,
            int,
            str,
            bool,
            tuple[str, ...],
        ]
    ] = []
    for entry in inventory.entries:
        snapshot = by_code[entry.product_code]
        if snapshot.request.uri != entry.source_uri:
            raise ValueError(
                "Eurostat unemployment release request identity differs"
            )
        artifact = _artifact_from_snapshot(
            snapshot,
            EurostatUnemploymentArtifactRole.RELEASE_HTML,
            product_code=entry.product_code,
        )
        _, page_date, published_at, document_uris = _parse_release_landing(
            snapshot, entry
        )
        document_artifact: EurostatUnemploymentArtifactV1 | None = None
        document_snapshot: OfficialRawSnapshotV1 | None = None
        if document_uris:
            document_uri = document_uris[0]
            try:
                document_snapshot = documents_by_uri[document_uri]
            except KeyError as exc:
                raise ValueError(
                    "Eurostat unemployment document corpus is incomplete"
                ) from exc
            used_document_uris.add(document_uri)
            document_role = {
                OfficialSourceFormat.HTML: (
                    EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_HTML
                ),
                OfficialSourceFormat.PDF: (
                    EurostatUnemploymentArtifactRole.RELEASE_DOCUMENT_PDF
                ),
            }.get(document_snapshot.request.source_format)
            if document_role is None:
                raise ValueError(
                    "Eurostat unemployment document format differs"
                )
            document_artifact = _artifact_from_snapshot(
                document_snapshot,
                document_role,
                product_code=entry.product_code,
            )
        reference_period, lag = _reference_period_from_publication(
            entry, snapshot, document_snapshot
        )
        composition, composition_source_stated = _composition_from_publication(
            reference_period, snapshot, document_snapshot
        )
        parsed.append(
            (
                entry,
                artifact,
                document_artifact,
                page_date,
                published_at,
                reference_period,
                lag,
                composition,
                composition_source_stated,
                document_uris,
            )
        )
    if used_document_uris != set(documents_by_uri):
        raise ValueError(
            "Eurostat unemployment document corpus has unexpected artifacts"
        )
    releases: list[EurostatUnemploymentReleaseV1] = []
    for (
        entry,
        artifact,
        document_artifact,
        page_date,
        published_at,
        reference_period,
        lag,
        composition,
        composition_source_stated,
        document_uris,
    ) in parsed:
        expected_lag = _release_lag(entry.release_date, reference_period)
        if lag != expected_lag:
            raise ValueError(
                "Eurostat unemployment release lag changed during parsing"
            )
        headline = _headline_value(entry, reference_period)
        document_snapshot = (
            documents_by_uri[document_uris[0]] if document_uris else None
        )
        releases.append(
            EurostatUnemploymentReleaseV1(
                inventory_entry=entry,
                reference_period=reference_period,
                days_after_period_end=lag,
                euro_area_composition=composition,
                composition_source_stated=composition_source_stated,
                page_release_date=page_date,
                page_release_date_offset_days=(
                    date.fromisoformat(page_date)
                    - date.fromisoformat(entry.release_date)
                ).days,
                published_at=published_at,
                linked_document_uris=document_uris,
                artifact=artifact,
                document_artifact=document_artifact,
                headline_value=headline,
                published_values=_published_values(
                    by_code[entry.product_code],
                    artifact,
                    document_snapshot,
                    document_artifact,
                    reference_period,
                    headline,
                ),
            )
        )
    artifacts = (
        *inventory.artifacts,
        dataset.artifact,
        *(item.artifact for item in releases),
        *(
            item.document_artifact
            for item in releases
            if item.document_artifact is not None
        ),
    )
    revision_comparison_count, changed_revision_count = (
        _published_revision_counts(releases)
    )
    current_dataset_comparison_count, changed_current_dataset_count = (
        _current_dataset_comparison_counts(releases, dataset)
    )
    return EurostatUnemploymentArchiveManifestV1(
        as_of_date=as_of_date,
        registry_id=registry.registry_id,
        source_id=source.source_id,
        inventory=inventory,
        current_dataset=dataset,
        releases=tuple(releases),
        searchable_release_gap_reference_periods=(
            *_month_range(
                dataset.time_start,
                _month_shift(EUROSTAT_UNEMPLOYMENT_FIRST_REFERENCE_PERIOD, -1),
            ),
            *EUROSTAT_UNEMPLOYMENT_INTERNAL_GAP_REFERENCE_PERIODS,
        ),
        composition_source_stated_count=sum(
            item.composition_source_stated for item in releases
        ),
        page_date_offset_count=sum(
            item.page_release_date_offset_days != 0 for item in releases
        ),
        exact_publication_time_count=sum(
            item.published_at is not None for item in releases
        ),
        release_document_count=sum(
            item.document_artifact is not None for item in releases
        ),
        published_value_count=sum(
            len(item.published_values) for item in releases
        ),
        revision_comparison_count=revision_comparison_count,
        changed_revision_count=changed_revision_count,
        current_dataset_comparison_count=current_dataset_comparison_count,
        changed_current_dataset_count=changed_current_dataset_count,
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
    )


def replay_eurostat_unemployment_archive(
    registry: OfficialSourceRegistryV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    dataset_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Sequence[OfficialRawSnapshotV1],
    expected_manifest: EurostatUnemploymentArchiveManifestV1,
) -> EurostatUnemploymentArchiveManifestV1:
    """Rebuild the unemployment receipt and require byte-for-byte identity."""
    rebuilt = build_eurostat_unemployment_archive_manifest(
        registry,
        search_snapshots,
        dataset_snapshot,
        release_snapshots,
        document_snapshots,
        as_of_date=expected_manifest.as_of_date,
    )
    if rebuilt.to_json() != expected_manifest.to_json():
        raise ValueError(
            "Eurostat unemployment replay differs from packaged manifest"
        )
    return rebuilt


def packaged_eurostat_unemployment_manifest_path() -> Path:
    """Return the installed unemployment archive-manifest path."""
    return (
        Path(__file__).resolve().parent
        / "assets"
        / "eurostat_unemployment_archive_v1.json"
    )


def load_packaged_eurostat_unemployment_archive_manifest() -> (
    EurostatUnemploymentArchiveManifestV1
):
    """Load and validate the installed unemployment archive manifest."""
    path = packaged_eurostat_unemployment_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError(
            "packaged Eurostat unemployment manifest exceeds size bound"
        )
    manifest = EurostatUnemploymentArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_UNEMPLOYMENT_SOURCE_KEY)
    if (
        manifest.registry_id != registry.registry_id
        or manifest.source_id != source.source_id
    ):
        raise ValueError(
            "packaged Eurostat unemployment registry binding differs"
        )
    return manifest


__all__ = [
    "EUROSTAT_UNEMPLOYMENT_ARTIFACT_SCHEMA_VERSION",
    "EUROSTAT_UNEMPLOYMENT_AS_OF_DATE",
    "EUROSTAT_UNEMPLOYMENT_DATASET_SCHEMA_VERSION",
    "EUROSTAT_UNEMPLOYMENT_DATASET_URI",
    "EUROSTAT_UNEMPLOYMENT_DOCUMENT_COUNT",
    "EUROSTAT_UNEMPLOYMENT_EXCLUSION_SCHEMA_VERSION",
    "EUROSTAT_UNEMPLOYMENT_FIRST_REFERENCE_PERIOD",
    "EUROSTAT_UNEMPLOYMENT_FIRST_SEARCH_RELEASE_DATE",
    "EUROSTAT_UNEMPLOYMENT_INTERNAL_GAP_REFERENCE_PERIODS",
    "EUROSTAT_UNEMPLOYMENT_INVENTORY_ENTRY_SCHEMA_VERSION",
    "EUROSTAT_UNEMPLOYMENT_INVENTORY_SCHEMA_VERSION",
    "EUROSTAT_UNEMPLOYMENT_LATEST_PACKAGED_RELEASE_DATE",
    "EUROSTAT_UNEMPLOYMENT_LATEST_REFERENCE_PERIOD",
    "EUROSTAT_UNEMPLOYMENT_MANIFEST_SCHEMA_VERSION",
    "EUROSTAT_UNEMPLOYMENT_MEASURE_KEY",
    "EUROSTAT_UNEMPLOYMENT_OBSERVATION_SCHEMA_VERSION",
    "EUROSTAT_UNEMPLOYMENT_PAGE_DATE_OFFSET_EXCEPTIONS",
    "EUROSTAT_UNEMPLOYMENT_PARSER_ID",
    "EUROSTAT_UNEMPLOYMENT_PARSER_VERSION",
    "EUROSTAT_UNEMPLOYMENT_PROGRAM_KEY",
    "EUROSTAT_UNEMPLOYMENT_PUBLISHED_VALUE_SCHEMA_VERSION",
    "EUROSTAT_UNEMPLOYMENT_RELEASE_COUNT",
    "EUROSTAT_UNEMPLOYMENT_RELEASE_SCHEMA_VERSION",
    "EUROSTAT_UNEMPLOYMENT_RELEASE_VALUE_SCHEMA_VERSION",
    "EUROSTAT_UNEMPLOYMENT_SEARCH_PAGE_COUNT",
    "EUROSTAT_UNEMPLOYMENT_SEARCH_RESULT_COUNT",
    "EUROSTAT_UNEMPLOYMENT_SEARCH_URI",
    "EUROSTAT_UNEMPLOYMENT_SOURCE_KEY",
    "EUROSTAT_UNEMPLOYMENT_SOURCE_TIMEZONE",
    "EurostatUnemploymentArchiveManifestV1",
    "EurostatUnemploymentArtifactRole",
    "EurostatUnemploymentArtifactV1",
    "EurostatUnemploymentDatasetV1",
    "EurostatUnemploymentIndexExclusionV1",
    "EurostatUnemploymentInventoryEntryV1",
    "EurostatUnemploymentInventoryV1",
    "EurostatUnemploymentMeasure",
    "EurostatUnemploymentMovement",
    "EurostatUnemploymentObservationV1",
    "EurostatUnemploymentPublishedValueV1",
    "EurostatUnemploymentReleaseV1",
    "EurostatUnemploymentReleaseValueV1",
    "build_eurostat_unemployment_archive_manifest",
    "build_eurostat_unemployment_dataset_request",
    "build_eurostat_unemployment_document_requests",
    "build_eurostat_unemployment_release_requests",
    "build_eurostat_unemployment_search_requests",
    "load_packaged_eurostat_unemployment_archive_manifest",
    "packaged_eurostat_unemployment_manifest_path",
    "parse_eurostat_unemployment_dataset",
    "parse_eurostat_unemployment_inventory",
    "replay_eurostat_unemployment_archive",
]
