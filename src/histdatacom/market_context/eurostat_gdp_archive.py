"""Deterministic qualification of Eurostat quarterly GDP releases.

Eurostat's searchable Euro-indicator feed is the release inventory, while
source-dated HTML/PDF products are the vintage evidence.  The current
``namq_10_gdp`` JSON-stat table is retained only as a revised-series
cross-check; it never substitutes for values as published in a release.
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
from datetime import date, datetime
from enum import Enum
from html.parser import HTMLParser
from io import BytesIO
from itertools import pairwise
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit
from xml.etree.ElementTree import Element, ParseError

from defusedxml.common import DefusedXmlException
from defusedxml.ElementTree import fromstring as safe_xml_fromstring
from pypdf import PdfReader
from pypdf.errors import PdfReadError

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

EUROSTAT_GDP_SOURCE_KEY: Final = "ea.eurostat.gdp"
EUROSTAT_GDP_PROGRAM_KEY: Final = "ea.eurostat.quarterly-gdp"
EUROSTAT_GDP_MEASURE_KEY: Final = "real-gdp-growth"
EUROSTAT_GDP_PARSER_ID: Final = "official.eurostat-gdp.v1"
EUROSTAT_GDP_PARSER_VERSION: Final = "1"
EUROSTAT_GDP_SOURCE_TIMEZONE: Final = "Europe/Brussels"
EUROSTAT_GDP_AS_OF_DATE: Final = "2026-09-15"
EUROSTAT_GDP_FIRST_SEARCH_RELEASE_DATE: Final = "2002-01-10"
EUROSTAT_GDP_LATEST_PACKAGED_RELEASE_DATE: Final = "2026-09-07"
EUROSTAT_GDP_SEARCH_PAGE_COUNT: Final = 10
EUROSTAT_GDP_SEARCH_RESULT_COUNT: Final = 932
EUROSTAT_GDP_TITLE_MATCH_COUNT: Final = 521
EUROSTAT_GDP_PREFILTER_COUNT: Final = 280
EUROSTAT_GDP_RELEASE_COUNT: Final = 279
EUROSTAT_GDP_DOCUMENT_COUNT: Final = 248

EUROSTAT_GDP_SEARCH_URI: Final = "https://ec.europa.eu/eurostat/search"
EUROSTAT_GDP_PRODUCT_URI_TEMPLATE: Final = (
    "https://ec.europa.eu/eurostat/product?code={product_code}"
)
EUROSTAT_GDP_DATASET_URI: Final = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "namq_10_gdp?lang=en&freq=Q&unit=CLV_PCH_PRE&unit=CLV_PCH_SM&"
    "s_adj=SCA&na_item=B1GQ&geo=EA20&geo=DE&geo=FR&"
    "sinceTimePeriod=2000-Q1"
)

EUROSTAT_GDP_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-gdp-artifact.v1"
)
EUROSTAT_GDP_INVENTORY_ENTRY_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-gdp-inventory-entry.v1"
)
EUROSTAT_GDP_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-gdp-exclusion.v1"
)
EUROSTAT_GDP_INVENTORY_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-gdp-inventory.v1"
)
EUROSTAT_GDP_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-gdp-observation.v1"
)
EUROSTAT_GDP_DATASET_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-gdp-dataset.v1"
)
EUROSTAT_GDP_RELEASE_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-gdp-release-value.v1"
)
EUROSTAT_GDP_PUBLISHED_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-gdp-published-value.v1"
)
EUROSTAT_GDP_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-gdp-release.v1"
)
EUROSTAT_GDP_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-gdp-archive-manifest.v1"
)

MAX_EUROSTAT_GDP_INDEX_PAGES: Final = 32
MAX_EUROSTAT_GDP_SEARCH_ENTRIES: Final = 2_048
MAX_EUROSTAT_GDP_RELEASES: Final = 512
MAX_EUROSTAT_GDP_OBSERVATIONS: Final = 4_096
MAX_EUROSTAT_GDP_ARTIFACTS: Final = 2_048
MAX_EUROSTAT_GDP_TOTAL_BYTES: Final = 1_024_000_000
MAX_EUROSTAT_GDP_TITLE_CHARS: Final = 512

_SEARCH_PREFIX: Final = (
    "_estatsearchportlet_WAR_estatsearchportlet_INSTANCE_bHVzuvn1SZ8J_"
)
_ATOM_NAMESPACE: Final = "http://www.w3.org/2005/Atom"
_PRODUCT_CODE_RE = re.compile(
    r"(?P<prefix>[234])-(?P<day>\d{2})(?P<month>\d{2})(?P<year>\d{4})-"
    r"(?P<suffix>ap(?:\d+|_\d+)?|bp\d*|cp\d*)",
    re.IGNORECASE,
)
_QUARTER_RE = re.compile(r"\d{4}-Q[1-4]")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")
_SIGNED_NUMBER_RE = re.compile(r"[+-]?\d+(?:\.\d+)?")
_SPACE_RE = re.compile(r"\s+")
_ECONOMIES: Final = ("EA20", "DE", "FR")
_ECONOMY_ORDER: Final = {code: index for index, code in enumerate(_ECONOMIES)}
_RELEASE_ECONOMIES: Final = ("EA", "DE", "FR")
_RELEASE_ECONOMY_ORDER: Final = {
    code: index for index, code in enumerate(_RELEASE_ECONOMIES)
}
_EXPLICIT_EXCLUSIONS: Final[Mapping[str, str]] = {
    "2-17102014-bp": (
        "ESA 2010 level-shift methodology item is not a quarterly GDP release"
    )
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
        parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include an offset")
    return result


def _quarter(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _QUARTER_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-QN")
    return result


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
                "Eurostat GDP product URI has no unique product code"
            )
        token = candidates[0]
    else:
        token = parsed.path if parsed.scheme else value
    match = _PRODUCT_CODE_RE.search(token)
    if match is None:
        raise ValueError("Eurostat GDP product code is invalid")
    return match.group(0).casefold()


def _product_release_date(product_code: object) -> str:
    match = _PRODUCT_CODE_RE.fullmatch(_product_code(product_code))
    if match is None:  # pragma: no cover - guarded by _product_code
        raise ValueError("Eurostat GDP product code is invalid")
    token = f"{match.group('year')}-{match.group('month')}-{match.group('day')}"
    return _iso_date(token, "release_date")


def _inventory_prefilter(title: str) -> bool:
    if re.search(r"\bGDP\b", title, re.IGNORECASE) is None:
        return False
    if (
        re.search(
            r"\b(?:euro area|euro-zone|euro zone)\b", title, re.IGNORECASE
        )
        is None
    ):
        return False
    if (
        re.search(
            r"\b(?:up|down|stable|unchanged|grew|growth|contract|fell|rose|"
            r"increased|decreased)\b",
            title,
            re.IGNORECASE,
        )
        is None
    ):
        return False
    return (
        re.search(
            r"\b(?:government|debt|deficit|per capita|regions?|greenhouse|energy|"
            r"house prices?|current account|tax|employment rate)\b",
            title,
            re.IGNORECASE,
        )
        is None
    )


def _inventory_selected(title: str, product_code: str) -> bool:
    return (
        _inventory_prefilter(title) and product_code not in _EXPLICIT_EXCLUSIONS
    )


class EurostatGdpArtifactRole(str, Enum):
    """Role of one exact official byte artifact."""

    SEARCH_INDEX = "search-index"
    CURRENT_DATASET = "current-dataset"
    RELEASE_HTML = "release-html"
    RELEASE_DOCUMENT_HTML = "release-document-html"
    RELEASE_DOCUMENT_PDF = "release-document-pdf"

    @classmethod
    def from_value(
        cls, value: str | EurostatGdpArtifactRole
    ) -> EurostatGdpArtifactRole:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError("unsupported Eurostat GDP artifact role") from exc


class EurostatGdpReleaseStage(str, Enum):
    """Evidence-graded quarterly GDP publication stage."""

    PRELIMINARY_FLASH = "preliminary-flash"
    FLASH = "flash"
    FIRST_ESTIMATE = "first-estimate"
    SECOND_ESTIMATE = "second-estimate"
    THIRD_ESTIMATE = "third-estimate"
    REGULAR_ESTIMATE = "regular-estimate"

    @classmethod
    def from_value(
        cls, value: str | EurostatGdpReleaseStage
    ) -> EurostatGdpReleaseStage:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError("unsupported Eurostat GDP release stage") from exc


class EurostatGdpGrowthMeasure(str, Enum):
    """Unambiguous quarterly real-GDP growth comparison."""

    QUARTER_OVER_QUARTER = "quarter-over-quarter"
    YEAR_OVER_YEAR = "year-over-year"

    @classmethod
    def from_value(
        cls, value: str | EurostatGdpGrowthMeasure
    ) -> EurostatGdpGrowthMeasure:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError("unsupported Eurostat GDP growth measure") from exc


@dataclass(frozen=True, slots=True)
class EurostatGdpArtifactV1:
    """Content-addressed official GDP evidence artifact."""

    role: EurostatGdpArtifactRole
    request_uri: str
    resolved_uri: str
    source_format: OfficialSourceFormat
    content_length: int
    content_sha256: str
    page_number: int | None = None
    product_code: str | None = None
    schema_version: str = EUROSTAT_GDP_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_GDP_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat GDP artifact schema")
        role = EurostatGdpArtifactRole.from_value(self.role)
        request_uri = _https_uri(self.request_uri, "request_uri")
        resolved_uri = _https_uri(self.resolved_uri, "resolved_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        expected_format = {
            EurostatGdpArtifactRole.SEARCH_INDEX: OfficialSourceFormat.ATOM,
            EurostatGdpArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
            EurostatGdpArtifactRole.RELEASE_HTML: OfficialSourceFormat.HTML,
            EurostatGdpArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
            EurostatGdpArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
        }[role]
        if source_format is not expected_format:
            raise ValueError("Eurostat GDP artifact role and format differ")
        length = _bounded_int(
            self.content_length, "content_length", MAX_EUROSTAT_GDP_TOTAL_BYTES
        )
        if length < 1:
            raise ValueError("Eurostat GDP artifact is empty")
        page_number = self.page_number
        product_code = self.product_code
        if role is EurostatGdpArtifactRole.SEARCH_INDEX:
            if page_number is None:
                raise ValueError(
                    "Eurostat GDP search artifact needs a page number"
                )
            _bounded_int(
                page_number, "page_number", MAX_EUROSTAT_GDP_INDEX_PAGES
            )
            if product_code is not None:
                raise ValueError(
                    "Eurostat GDP search artifact has a product code"
                )
        elif role is EurostatGdpArtifactRole.CURRENT_DATASET:
            if page_number is not None or product_code is not None:
                raise ValueError(
                    "Eurostat GDP dataset artifact has release metadata"
                )
        else:
            if page_number is not None or product_code is None:
                raise ValueError(
                    "Eurostat GDP release artifact metadata differs"
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatGdpArtifactV1:
        return cls(
            role=EurostatGdpArtifactRole.from_value(str(data.get("role", ""))),
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
class EurostatGdpInventoryEntryV1:
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
    schema_version: str = EUROSTAT_GDP_INVENTORY_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_GDP_INVENTORY_ENTRY_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat GDP inventory-entry schema")
        product_code = _product_code(self.product_code)
        release_date = _iso_date(self.release_date, "release_date")
        if release_date != _product_release_date(product_code):
            raise ValueError(
                "Eurostat GDP release date differs from product code"
            )
        title = _required_text(
            self.source_title,
            "source_title",
            maximum=MAX_EUROSTAT_GDP_TITLE_CHARS,
        )
        if not _inventory_selected(title, product_code):
            raise ValueError(
                "Eurostat GDP inventory entry is outside selection"
            )
        source_uri = _https_uri(self.source_uri, "source_uri")
        parsed_source_uri = urlsplit(source_uri)
        if (
            parsed_source_uri.path != "/eurostat/product"
            or set(parse_qs(parsed_source_uri.query)) != {"code"}
            or parsed_source_uri.fragment
        ):
            raise ValueError("Eurostat GDP source URI route differs")
        if _product_code(source_uri) != product_code:
            raise ValueError("Eurostat GDP source URI and product code differ")
        page = _bounded_int(
            self.page_number, "page_number", MAX_EUROSTAT_GDP_INDEX_PAGES
        )
        if page < 1:
            raise ValueError("Eurostat GDP page number must be positive")
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
            "eurostat-gdp-inventory-entry", self.identity_payload()
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError("Eurostat GDP inventory-entry identity differs")
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatGdpInventoryEntryV1:
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
class EurostatGdpIndexExclusionV1:
    """One explicit title-prefilter false positive."""

    product_code: str
    release_date: str
    source_title: str
    source_uri: str
    reason: str
    exclusion_id: str = ""
    schema_version: str = EUROSTAT_GDP_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_GDP_EXCLUSION_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat GDP exclusion schema")
        product_code = _product_code(self.product_code)
        if product_code not in _EXPLICIT_EXCLUSIONS:
            raise ValueError("unexpected Eurostat GDP explicit exclusion")
        release_date = _iso_date(self.release_date, "release_date")
        if release_date != _product_release_date(product_code):
            raise ValueError("Eurostat GDP exclusion date differs")
        title = _required_text(
            self.source_title,
            "source_title",
            maximum=MAX_EUROSTAT_GDP_TITLE_CHARS,
        )
        source_uri = _https_uri(self.source_uri, "source_uri")
        reason = _required_text(self.reason, "reason")
        if reason != _EXPLICIT_EXCLUSIONS[product_code]:
            raise ValueError("Eurostat GDP exclusion reason differs")
        object.__setattr__(self, "product_code", product_code)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "source_uri", source_uri)
        object.__setattr__(self, "reason", reason)
        expected = _stable_id("eurostat-gdp-exclusion", self.identity_payload())
        if self.exclusion_id and self.exclusion_id != expected:
            raise ValueError("Eurostat GDP exclusion identity differs")
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatGdpIndexExclusionV1:
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
class EurostatGdpInventoryV1:
    """Hash-bound official search inventory and deterministic selection."""

    artifacts: tuple[EurostatGdpArtifactV1, ...]
    entries: tuple[EurostatGdpInventoryEntryV1, ...]
    exclusions: tuple[EurostatGdpIndexExclusionV1, ...]
    query_result_count: int
    title_match_count: int
    prefilter_count: int
    inventory_id: str = ""
    schema_version: str = EUROSTAT_GDP_INVENTORY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_GDP_INVENTORY_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat GDP inventory schema")
        artifacts = tuple(self.artifacts)
        entries = tuple(self.entries)
        exclusions = tuple(self.exclusions)
        if len(artifacts) != EUROSTAT_GDP_SEARCH_PAGE_COUNT:
            raise ValueError("Eurostat GDP search page count differs")
        if any(
            item.role is not EurostatGdpArtifactRole.SEARCH_INDEX
            for item in artifacts
        ):
            raise ValueError("Eurostat GDP inventory artifact role differs")
        if [item.page_number for item in artifacts] != list(
            range(1, EUROSTAT_GDP_SEARCH_PAGE_COUNT + 1)
        ):
            raise ValueError("Eurostat GDP search pages are not contiguous")
        if len(entries) != EUROSTAT_GDP_RELEASE_COUNT:
            raise ValueError("Eurostat GDP selected release count differs")
        codes = [item.product_code for item in entries]
        if len(codes) != len(set(codes)):
            raise ValueError("Eurostat GDP inventory repeats a product code")
        if list(entries) != sorted(
            entries, key=lambda item: (item.release_date, item.product_code)
        ):
            raise ValueError("Eurostat GDP inventory entries are not canonical")
        if entries[0].release_date != EUROSTAT_GDP_FIRST_SEARCH_RELEASE_DATE:
            raise ValueError("Eurostat GDP first searchable release differs")
        if (
            entries[-1].release_date
            != EUROSTAT_GDP_LATEST_PACKAGED_RELEASE_DATE
        ):
            raise ValueError("Eurostat GDP latest searchable release differs")
        if len(exclusions) != len(_EXPLICIT_EXCLUSIONS):
            raise ValueError("Eurostat GDP exclusion count differs")
        if {item.product_code for item in exclusions} != set(
            _EXPLICIT_EXCLUSIONS
        ):
            raise ValueError("Eurostat GDP explicit exclusions differ")
        if set(codes) & {item.product_code for item in exclusions}:
            raise ValueError("Eurostat GDP selection includes an exclusion")
        expected_counts = (
            EUROSTAT_GDP_SEARCH_RESULT_COUNT,
            EUROSTAT_GDP_TITLE_MATCH_COUNT,
            EUROSTAT_GDP_PREFILTER_COUNT,
        )
        observed_counts = (
            _bounded_int(
                self.query_result_count,
                "query_result_count",
                MAX_EUROSTAT_GDP_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.title_match_count,
                "title_match_count",
                MAX_EUROSTAT_GDP_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.prefilter_count,
                "prefilter_count",
                MAX_EUROSTAT_GDP_SEARCH_ENTRIES,
            ),
        )
        if observed_counts != expected_counts:
            raise ValueError("Eurostat GDP search inventory counts differ")
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "exclusions", exclusions)
        expected = _stable_id("eurostat-gdp-inventory", self.identity_payload())
        if self.inventory_id and self.inventory_id != expected:
            raise ValueError("Eurostat GDP inventory identity differs")
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatGdpInventoryV1:
        return cls(
            artifacts=tuple(
                EurostatGdpArtifactV1.from_dict(_mapping(item, "artifact"))
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            entries=tuple(
                EurostatGdpInventoryEntryV1.from_dict(
                    _mapping(item, "inventory entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            exclusions=tuple(
                EurostatGdpIndexExclusionV1.from_dict(
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
class EurostatGdpObservationV1:
    """One current-revised JSON-stat GDP growth observation."""

    economy_code: str
    reference_period: str
    measure: EurostatGdpGrowthMeasure
    value_lexical: str
    value: float
    status: str | None
    flat_index: int
    observation_id: str = ""
    schema_version: str = EUROSTAT_GDP_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_GDP_OBSERVATION_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat GDP observation schema")
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _ECONOMIES:
            raise ValueError("Eurostat GDP observation economy differs")
        reference = _quarter(self.reference_period, "reference_period")
        measure = EurostatGdpGrowthMeasure.from_value(self.measure)
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError("Eurostat GDP observation lexical is invalid")
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError("Eurostat GDP observation numeric differs")
        if not -100.0 <= numeric <= 100.0:
            raise ValueError("Eurostat GDP observation is outside its bound")
        status = _optional_text(self.status, "status")
        if status is not None and len(status) > 16:
            raise ValueError("Eurostat GDP status exceeds its bound")
        flat_index = _bounded_int(
            self.flat_index, "flat_index", MAX_EUROSTAT_GDP_OBSERVATIONS * 4
        )
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "flat_index", flat_index)
        expected = _stable_id(
            "eurostat-gdp-observation", self.identity_payload()
        )
        if self.observation_id and self.observation_id != expected:
            raise ValueError("Eurostat GDP observation identity differs")
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatGdpObservationV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatGdpGrowthMeasure.from_value(
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
class EurostatGdpDatasetV1:
    """Exact current-revised ``namq_10_gdp`` cross-check response."""

    label: str
    updated_at: str
    time_start: str
    time_end: str
    artifact: EurostatGdpArtifactV1
    observations: tuple[EurostatGdpObservationV1, ...]
    dataset_id: str = "namq_10_gdp"
    dataset_receipt_id: str = ""
    schema_version: str = EUROSTAT_GDP_DATASET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_GDP_DATASET_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat GDP dataset schema")
        if self.dataset_id != "namq_10_gdp":
            raise ValueError("unsupported Eurostat GDP dataset")
        label = _required_text(self.label, "label")
        updated = _iso_datetime(self.updated_at, "updated_at")
        time_start = _quarter(self.time_start, "time_start")
        time_end = _quarter(self.time_end, "time_end")
        if time_end < time_start:
            raise ValueError("Eurostat GDP dataset time range is reversed")
        if self.artifact.role is not EurostatGdpArtifactRole.CURRENT_DATASET:
            raise ValueError("Eurostat GDP dataset artifact role differs")
        observations = tuple(self.observations)
        if not 1 <= len(observations) <= MAX_EUROSTAT_GDP_OBSERVATIONS:
            raise ValueError("Eurostat GDP observation count is invalid")
        keys = [item.key for item in observations]
        if len(keys) != len(set(keys)):
            raise ValueError("Eurostat GDP dataset repeats an observation")
        periods = _quarter_range(time_start, time_end)
        expected_keys = {
            (measure.value, economy, period)
            for measure in EurostatGdpGrowthMeasure
            for economy in _ECONOMIES
            for period in periods
        }
        if set(keys) != expected_keys:
            raise ValueError("Eurostat GDP dataset cube coverage differs")
        measure_order = {
            EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER: 0,
            EurostatGdpGrowthMeasure.YEAR_OVER_YEAR: 1,
        }
        if list(observations) != sorted(
            observations,
            key=lambda item: (
                measure_order[item.measure],
                _ECONOMY_ORDER[item.economy_code],
                item.reference_period,
            ),
        ):
            raise ValueError("Eurostat GDP observations are not canonical")
        if (
            min(item.reference_period for item in observations) != time_start
            or max(item.reference_period for item in observations) != time_end
        ):
            raise ValueError("Eurostat GDP dataset time range differs")
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "updated_at", updated)
        object.__setattr__(self, "time_start", time_start)
        object.__setattr__(self, "time_end", time_end)
        object.__setattr__(self, "observations", observations)
        expected = _stable_id("eurostat-gdp-dataset", self.identity_payload())
        if self.dataset_receipt_id and self.dataset_receipt_id != expected:
            raise ValueError("Eurostat GDP dataset identity differs")
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatGdpDatasetV1:
        return cls(
            label=str(data.get("label", "")),
            updated_at=str(data.get("updated_at", "")),
            time_start=str(data.get("time_start", "")),
            time_end=str(data.get("time_end", "")),
            artifact=EurostatGdpArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            observations=tuple(
                EurostatGdpObservationV1.from_dict(
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
        page_number, "page_number", MAX_EUROSTAT_GDP_INDEX_PAGES
    )
    if page < 1:
        raise ValueError("Eurostat GDP search page must be positive")
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
        f"{_SEARCH_PREFIX}text": "GDP",
        f"{_SEARCH_PREFIX}sort": "date",
        f"{_SEARCH_PREFIX}collection": "CAT_PREREL",
        f"{_SEARCH_PREFIX}priv_r_p_implicitModel": "true",
        "pageNumber": str(page),
    }
    return f"{EUROSTAT_GDP_SEARCH_URI}?{urlencode(params)}"


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(EUROSTAT_GDP_SOURCE_KEY)
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


def build_eurostat_gdp_search_requests(
    registry: OfficialSourceRegistryV1,
    *,
    page_count: int = EUROSTAT_GDP_SEARCH_PAGE_COUNT,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the bounded official Atom inventory requests."""
    count = _bounded_int(page_count, "page_count", MAX_EUROSTAT_GDP_INDEX_PAGES)
    if count < 1:
        raise ValueError("Eurostat GDP search needs at least one page")
    return tuple(
        _request(
            registry,
            _search_uri(page),
            OfficialSourceFormat.ATOM,
            page_number=page,
        )
        for page in range(1, count + 1)
    )


def build_eurostat_gdp_dataset_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact current-revised GDP cross-check request."""
    return _request(
        registry,
        EUROSTAT_GDP_DATASET_URI,
        OfficialSourceFormat.JSON_STAT,
    )


def build_eurostat_gdp_release_requests(
    registry: OfficialSourceRegistryV1,
    inventory: EurostatGdpInventoryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one primary request for every selected GDP product."""
    if not isinstance(inventory, EurostatGdpInventoryV1):
        raise TypeError("Eurostat GDP release requests require a v1 inventory")
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
    raise ValueError("Eurostat GDP release document format differs")


def build_eurostat_gdp_document_requests(
    registry: OfficialSourceRegistryV1,
    inventory: EurostatGdpInventoryV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Discover one English source document for each legacy landing."""
    if not isinstance(inventory, EurostatGdpInventoryV1):
        raise TypeError("Eurostat GDP document requests require a v1 inventory")
    entries = {item.product_code: item for item in inventory.entries}
    snapshots = tuple(release_snapshots)
    if len(snapshots) != EUROSTAT_GDP_RELEASE_COUNT:
        raise ValueError("Eurostat GDP landing snapshot count differs")
    by_code: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        code = _product_code(snapshot.request.uri)
        if code in by_code:
            raise ValueError(
                "Eurostat GDP landing corpus repeats a product code"
            )
        by_code[code] = snapshot
    if set(by_code) != set(entries):
        raise ValueError("Eurostat GDP landing corpus is incomplete")
    requests: list[OfficialSourceRequestV1] = []
    for code, entry in entries.items():
        snapshot = by_code[code]
        if snapshot.request.uri != entry.source_uri:
            raise ValueError("Eurostat GDP landing request identity differs")
        _, _, _, document_uris = _parse_release_landing(snapshot, entry)
        requests.extend(
            _request(registry, uri, _document_source_format(uri))
            for uri in document_uris
        )
    if len(requests) != EUROSTAT_GDP_DOCUMENT_COUNT:
        raise ValueError("Eurostat GDP English document count differs")
    return tuple(requests)


def _artifact_from_snapshot(
    snapshot: OfficialRawSnapshotV1,
    role: EurostatGdpArtifactRole,
    *,
    product_code: str | None = None,
) -> EurostatGdpArtifactV1:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("Eurostat GDP artifact requires a v1 snapshot")
    if (
        snapshot.request.source_key != EUROSTAT_GDP_SOURCE_KEY
        or snapshot.request.parser_id != EUROSTAT_GDP_PARSER_ID
        or snapshot.request.parser_version != EUROSTAT_GDP_PARSER_VERSION
    ):
        raise ValueError("Eurostat GDP snapshot source or parser differs")
    if snapshot.request.method is not OfficialRequestMethod.GET:
        raise ValueError("Eurostat GDP snapshot method differs")
    expected_format = {
        EurostatGdpArtifactRole.SEARCH_INDEX: OfficialSourceFormat.ATOM,
        EurostatGdpArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
        EurostatGdpArtifactRole.RELEASE_HTML: OfficialSourceFormat.HTML,
        EurostatGdpArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
        EurostatGdpArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
    }[role]
    if snapshot.request.source_format is not expected_format:
        raise ValueError(
            "Eurostat GDP snapshot format differs from artifact role"
        )
    if snapshot.status_code != 200:
        raise ValueError("Eurostat GDP snapshot status differs")
    if role is EurostatGdpArtifactRole.SEARCH_INDEX:
        if (
            snapshot.request.page_number is None
            or snapshot.request.uri != _search_uri(snapshot.request.page_number)
        ):
            raise ValueError("Eurostat GDP search request identity differs")
        try:
            root = safe_xml_fromstring(snapshot.content)
        except (DefusedXmlException, ParseError) as exc:
            raise ValueError(
                "Eurostat GDP search artifact is invalid XML"
            ) from exc
        if root.tag != f"{{{_ATOM_NAMESPACE}}}feed":
            raise ValueError("Eurostat GDP search artifact is not Atom")
    elif role is EurostatGdpArtifactRole.CURRENT_DATASET:
        if (
            snapshot.request.page_number is not None
            or snapshot.request.uri != EUROSTAT_GDP_DATASET_URI
        ):
            raise ValueError("Eurostat GDP dataset request identity differs")
        if not snapshot.content.lstrip().startswith(b"{"):
            raise ValueError("Eurostat GDP dataset artifact is not JSON")
    elif snapshot.request.page_number is not None:
        raise ValueError("Eurostat GDP release request has a page number")
    elif role in {
        EurostatGdpArtifactRole.RELEASE_HTML,
        EurostatGdpArtifactRole.RELEASE_DOCUMENT_HTML,
    }:
        if b"<html" not in snapshot.content[:16_384].lower():
            raise ValueError("Eurostat GDP HTML signature differs")
    elif not snapshot.content.startswith(b"%PDF-"):
        raise ValueError("Eurostat GDP PDF signature differs")
    if role in {
        EurostatGdpArtifactRole.RELEASE_DOCUMENT_HTML,
        EurostatGdpArtifactRole.RELEASE_DOCUMENT_PDF,
    } and (
        product_code is None
        or _product_code(snapshot.request.uri) != _product_code(product_code)
    ):
        raise ValueError("Eurostat GDP document product code differs")
    return EurostatGdpArtifactV1(
        role=role,
        request_uri=snapshot.request.uri,
        resolved_uri=snapshot.resolved_uri,
        source_format=expected_format,
        content_length=len(snapshot.content),
        content_sha256=snapshot.content_sha256,
        page_number=(
            snapshot.request.page_number
            if role is EurostatGdpArtifactRole.SEARCH_INDEX
            else None
        ),
        product_code=product_code,
    )


def _atom_text(entry: Element, tag: str) -> str:
    node = entry.find(f"{{{_ATOM_NAMESPACE}}}{tag}")
    if node is None or node.text is None:
        raise ValueError(f"Eurostat GDP Atom entry omits {tag}")
    return html.unescape(node.text)


def _atom_link(entry: Element) -> str:
    links = tuple(
        item
        for item in entry.findall(f"{{{_ATOM_NAMESPACE}}}link")
        if item.attrib.get("rel") == "alternate"
    )
    if len(links) != 1 or "href" not in links[0].attrib:
        raise ValueError("Eurostat GDP Atom entry alternate link differs")
    return _https_uri(links[0].attrib["href"], "source_uri")


def parse_eurostat_gdp_inventory(
    snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatGdpInventoryV1:
    """Parse and prove the complete bounded GDP search selection."""
    as_of = _iso_date(as_of_date, "as_of_date")
    values = tuple(snapshots)
    if len(values) != EUROSTAT_GDP_SEARCH_PAGE_COUNT:
        raise ValueError("Eurostat GDP inventory snapshot count differs")
    if [item.request.page_number for item in values] != list(
        range(1, EUROSTAT_GDP_SEARCH_PAGE_COUNT + 1)
    ):
        raise ValueError("Eurostat GDP search snapshots are not contiguous")
    artifacts: list[EurostatGdpArtifactV1] = []
    raw_entries: list[tuple[int, str, str, str, str, str]] = []
    for snapshot in values:
        artifact = _artifact_from_snapshot(
            snapshot, EurostatGdpArtifactRole.SEARCH_INDEX
        )
        artifacts.append(artifact)
        try:
            root = safe_xml_fromstring(snapshot.content)
        except (DefusedXmlException, ParseError) as exc:
            raise ValueError("Eurostat GDP search page is invalid XML") from exc
        entries = tuple(root.findall(f"{{{_ATOM_NAMESPACE}}}entry"))
        expected_count = 32 if artifact.page_number == 10 else 100
        if len(entries) != expected_count:
            raise ValueError("Eurostat GDP search page size differs")
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
    if len(raw_entries) != EUROSTAT_GDP_SEARCH_RESULT_COUNT:
        raise ValueError("Eurostat GDP search result count differs")
    uris = [item[2] for item in raw_entries]
    if len(uris) != len(set(uris)):
        raise ValueError("Eurostat GDP search inventory repeats a URI")
    title_match_count = sum(
        re.search(r"\bGDP\b", title, re.IGNORECASE) is not None
        for _, title, _, _, _, _ in raw_entries
    )
    prefiltered = [
        item for item in raw_entries if _inventory_prefilter(item[1])
    ]
    selected: list[EurostatGdpInventoryEntryV1] = []
    exclusions: list[EurostatGdpIndexExclusionV1] = []
    for page, title, uri, published, updated, summary in prefiltered:
        product_code = _product_code(uri)
        release_date = _product_release_date(product_code)
        if release_date > as_of:
            continue
        if product_code in _EXPLICIT_EXCLUSIONS:
            exclusions.append(
                EurostatGdpIndexExclusionV1(
                    product_code=product_code,
                    release_date=release_date,
                    source_title=title,
                    source_uri=uri,
                    reason=_EXPLICIT_EXCLUSIONS[product_code],
                )
            )
            continue
        selected.append(
            EurostatGdpInventoryEntryV1(
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
    return EurostatGdpInventoryV1(
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


def parse_eurostat_gdp_dataset(
    snapshot: OfficialRawSnapshotV1,
) -> EurostatGdpDatasetV1:
    """Decode the exact current-revised GDP JSON-stat cross-check."""
    artifact = _artifact_from_snapshot(
        snapshot, EurostatGdpArtifactRole.CURRENT_DATASET
    )
    try:
        payload = _mapping(json.loads(snapshot.content), "dataset")
        lexical_payload = _mapping(
            json.loads(snapshot.content, parse_float=str), "lexical dataset"
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Eurostat GDP dataset is invalid JSON") from exc
    identifiers = [str(item) for item in _sequence(payload.get("id"), "id")]
    expected_ids = ["freq", "unit", "s_adj", "na_item", "geo", "time"]
    if identifiers != expected_ids:
        raise ValueError("Eurostat GDP dataset dimensions differ")
    sizes = tuple(_sequence(payload.get("size"), "size"))
    if any(
        isinstance(item, bool) or not isinstance(item, int) for item in sizes
    ):
        raise TypeError("Eurostat GDP dimension sizes must be integers")
    dimension = _mapping(payload.get("dimension"), "dimension")
    expected_codes = {
        "freq": ("Q",),
        "unit": ("CLV_PCH_PRE", "CLV_PCH_SM"),
        "s_adj": ("SCA",),
        "na_item": ("B1GQ",),
        "geo": _ECONOMIES,
    }
    observed_codes = {
        name: _dimension_codes(_mapping(dimension[name], name), name)
        for name in expected_codes
    }
    if observed_codes != expected_codes:
        raise ValueError("Eurostat GDP dataset dimensions or codes differ")
    time_codes = _dimension_codes(_mapping(dimension["time"], "time"), "time")
    if not time_codes or any(
        _QUARTER_RE.fullmatch(item) is None for item in time_codes
    ):
        raise ValueError("Eurostat GDP dataset time codes differ")
    if time_codes != _quarter_range(time_codes[0], time_codes[-1]):
        raise ValueError("Eurostat GDP dataset time codes are not contiguous")
    expected_sizes = (1, 2, 1, 1, len(_ECONOMIES), len(time_codes))
    if tuple(cast(Sequence[int], sizes)) != expected_sizes:
        raise ValueError("Eurostat GDP dataset sizes differ")
    values = _mapping(payload.get("value"), "value")
    lexical_values = _mapping(lexical_payload.get("value"), "lexical value")
    statuses = _mapping(payload.get("status", {}), "status")
    if set(values) != set(lexical_values):
        raise ValueError("Eurostat GDP lexical and numeric values differ")
    if not set(statuses) <= set(values):
        raise ValueError("Eurostat GDP status has no observation")
    observations: list[EurostatGdpObservationV1] = []
    time_count = len(time_codes)
    geo_count = len(_ECONOMIES)
    maximum = 2 * geo_count * time_count
    for raw_index, raw_value in values.items():
        try:
            flat_index = int(str(raw_index))
        except ValueError as exc:
            raise ValueError("Eurostat GDP sparse index is invalid") from exc
        if not 0 <= flat_index < maximum:
            raise ValueError("Eurostat GDP sparse index is outside its cube")
        unit_index, remainder = divmod(flat_index, geo_count * time_count)
        geo_index, time_index = divmod(remainder, time_count)
        measure = (
            EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER
            if unit_index == 0
            else EurostatGdpGrowthMeasure.YEAR_OVER_YEAR
        )
        lexical = str(lexical_values[raw_index])
        observations.append(
            EurostatGdpObservationV1(
                economy_code=_ECONOMIES[geo_index],
                reference_period=time_codes[time_index],
                measure=measure,
                value_lexical=lexical,
                value=float(cast(float, raw_value)),
                status=cast(str | None, statuses.get(raw_index)),
                flat_index=flat_index,
            )
        )
    measure_order = {
        EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER: 0,
        EurostatGdpGrowthMeasure.YEAR_OVER_YEAR: 1,
    }
    observations.sort(
        key=lambda item: (
            measure_order[item.measure],
            _ECONOMY_ORDER[item.economy_code],
            item.reference_period,
        )
    )
    return EurostatGdpDatasetV1(
        label=str(payload.get("label", "")),
        updated_at=str(payload.get("updated", "")),
        time_start=time_codes[0],
        time_end=time_codes[-1],
        artifact=artifact,
        observations=tuple(observations),
    )


class EurostatGdpMovement(str, Enum):
    """Direction attached to the source-authored headline magnitude."""

    UP = "up"
    DOWN = "down"
    STABLE = "stable"

    @classmethod
    def from_value(
        cls, value: str | EurostatGdpMovement
    ) -> EurostatGdpMovement:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError("unsupported Eurostat GDP movement") from exc


@dataclass(frozen=True, slots=True)
class EurostatGdpReleaseValueV1:
    """Source-era euro-area GDP value in a release headline."""

    economy_code: str
    reference_period: str
    measure: EurostatGdpGrowthMeasure
    movement: EurostatGdpMovement
    magnitude_lexical: str
    value: float
    evidence_id: str
    release_value_id: str = ""
    schema_version: str = EUROSTAT_GDP_RELEASE_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_GDP_RELEASE_VALUE_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat GDP release-value schema")
        if self.economy_code != "EA":
            raise ValueError(
                "Eurostat GDP release value must retain source-era EA scope"
            )
        reference = _quarter(self.reference_period, "reference_period")
        measure = EurostatGdpGrowthMeasure.from_value(self.measure)
        if measure is not EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER:
            raise ValueError(
                "Eurostat GDP headline value must be quarter-over-quarter"
            )
        movement = EurostatGdpMovement.from_value(self.movement)
        lexical = _required_text(self.magnitude_lexical, "magnitude_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None or lexical.startswith("-"):
            raise ValueError("Eurostat GDP headline magnitude is invalid")
        magnitude = float(lexical)
        expected_value = {
            EurostatGdpMovement.UP: magnitude,
            EurostatGdpMovement.DOWN: -magnitude,
            EurostatGdpMovement.STABLE: 0.0,
        }[movement]
        if movement is EurostatGdpMovement.STABLE and magnitude != 0.0:
            raise ValueError("Eurostat GDP stable headline has a magnitude")
        if self.value != expected_value or not math.isfinite(self.value):
            raise ValueError("Eurostat GDP headline numeric value differs")
        if not -100.0 <= self.value <= 100.0:
            raise ValueError("Eurostat GDP headline value is outside its bound")
        evidence_id = _required_text(self.evidence_id, "evidence_id")
        if not evidence_id.startswith("eurostat-gdp-inventory-entry:sha256:"):
            raise ValueError("Eurostat GDP headline evidence differs")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "movement", movement)
        object.__setattr__(self, "magnitude_lexical", lexical)
        object.__setattr__(self, "evidence_id", evidence_id)
        expected = _stable_id(
            "eurostat-gdp-release-value", self.identity_payload()
        )
        if self.release_value_id and self.release_value_id != expected:
            raise ValueError("Eurostat GDP release-value identity differs")
        object.__setattr__(self, "release_value_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "reference_period": self.reference_period,
            "measure": self.measure.value,
            "movement": self.movement.value,
            "magnitude_lexical": self.magnitude_lexical,
            "value": self.value,
            "evidence_id": self.evidence_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "release_value_id": self.release_value_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatGdpReleaseValueV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatGdpGrowthMeasure.from_value(
                str(data.get("measure", ""))
            ),
            movement=EurostatGdpMovement.from_value(
                str(data.get("movement", ""))
            ),
            magnitude_lexical=str(data.get("magnitude_lexical", "")),
            value=float(cast(float, data.get("value"))),
            evidence_id=str(data.get("evidence_id", "")),
            release_value_id=str(data.get("release_value_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatGdpPublishedValueV1:
    """One source-era value read from a release publication table."""

    economy_code: str
    reference_period: str
    measure: EurostatGdpGrowthMeasure
    value_lexical: str
    value: float
    evidence_artifact_sha256: str
    evidence_locator: str
    published_value_id: str = ""
    schema_version: str = EUROSTAT_GDP_PUBLISHED_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_GDP_PUBLISHED_VALUE_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat GDP published-value schema")
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _RELEASE_ECONOMIES:
            raise ValueError("Eurostat GDP published-value economy differs")
        reference = _quarter(self.reference_period, "reference_period")
        measure = EurostatGdpGrowthMeasure.from_value(self.measure)
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _SIGNED_NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError("Eurostat GDP published-value lexical is invalid")
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError("Eurostat GDP published-value numeric differs")
        if not -100.0 <= numeric <= 100.0:
            raise ValueError(
                "Eurostat GDP published value is outside its bound"
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
            "eurostat-gdp-published-value", self.identity_payload()
        )
        if self.published_value_id and self.published_value_id != expected:
            raise ValueError("Eurostat GDP published-value identity differs")
        object.__setattr__(self, "published_value_id", expected)

    @property
    def key(self) -> tuple[str, str]:
        return self.economy_code, self.measure.value

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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatGdpPublishedValueV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatGdpGrowthMeasure.from_value(
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


def _quarter_shift(value: str, offset: int) -> str:
    token = _quarter(value, "reference_period")
    year = int(token[:4])
    quarter = int(token[-1])
    flat = year * 4 + quarter - 1 + offset
    shifted_year, zero_based_quarter = divmod(flat, 4)
    return f"{shifted_year:04d}-Q{zero_based_quarter + 1}"


def _quarter_range(start: str, end: str) -> tuple[str, ...]:
    first = _quarter(start, "start")
    last = _quarter(end, "end")
    if last < first:
        return ()
    result: list[str] = []
    current = first
    while current <= last:
        result.append(current)
        current = _quarter_shift(current, 1)
    return tuple(result)


def _quarter_end(value: str) -> date:
    token = _quarter(value, "reference_period")
    year = int(token[:4])
    month = int(token[-1]) * 3
    return date(year, month, monthrange(year, month)[1])


def _ea_composition_for_period(reference_period: str) -> str:
    reference = _quarter(reference_period, "reference_period")
    boundaries = (
        ("2006-Q4", "EA12"),
        ("2007-Q4", "EA13"),
        ("2008-Q4", "EA15"),
        ("2010-Q4", "EA16"),
        ("2013-Q4", "EA17"),
        ("2014-Q4", "EA18"),
        ("2022-Q4", "EA19"),
        ("9999-Q4", "EA20"),
    )
    return next(
        label for boundary, label in boundaries if reference <= boundary
    )


def _release_lag(release_date: str, reference_period: str) -> int:
    released = date.fromisoformat(_iso_date(release_date, "release_date"))
    reference = _quarter(reference_period, "reference_period")
    lag = (released - _quarter_end(reference)).days
    if not 20 <= lag <= 150:
        raise ValueError(
            "Eurostat GDP release lag is outside the qualified window"
        )
    return lag


def _release_stage(
    release_date: str,
    lag_days: int,
    period_lags: Sequence[int],
) -> EurostatGdpReleaseStage:
    released = date.fromisoformat(_iso_date(release_date, "release_date"))
    if lag_days <= 40:
        has_later_flash = any(lag_days < item <= 55 for item in period_lags)
        if has_later_flash or released >= date(2016, 4, 1):
            return EurostatGdpReleaseStage.PRELIMINARY_FLASH
        return EurostatGdpReleaseStage.FLASH
    if lag_days <= 55:
        return EurostatGdpReleaseStage.FLASH
    if lag_days <= 85:
        if released >= date(2011, 1, 1):
            return EurostatGdpReleaseStage.REGULAR_ESTIMATE
        return EurostatGdpReleaseStage.FIRST_ESTIMATE
    if lag_days <= 110:
        return EurostatGdpReleaseStage.SECOND_ESTIMATE
    return EurostatGdpReleaseStage.THIRD_ESTIMATE


_HEADLINE_VALUE_RE = re.compile(
    r"\bGDP\b(?:(?!\bGDP\b).){0,80}?"
    r"\b(?P<movement>up|down|stable|unchanged)\b"
    r"(?:\s+by)?(?:\s+(?P<magnitude>-?\d+(?:\.\d+)?))?",
    re.IGNORECASE,
)


def _headline_value(
    entry: EurostatGdpInventoryEntryV1,
    reference_period: str,
) -> EurostatGdpReleaseValueV1:
    match = _HEADLINE_VALUE_RE.search(entry.source_title)
    if match is None:
        raise ValueError("Eurostat GDP release headline has no GDP movement")
    movement_token = match.group("movement").casefold()
    movement = (
        EurostatGdpMovement.STABLE
        if movement_token in {"stable", "unchanged"}
        else EurostatGdpMovement.from_value(movement_token)
    )
    magnitude = match.group("magnitude")
    if movement is EurostatGdpMovement.STABLE:
        if magnitude is not None:
            raise ValueError(
                "Eurostat GDP stable headline unexpectedly has a value"
            )
        magnitude = "0"
    elif magnitude is None:
        raise ValueError("Eurostat GDP directional headline omits a value")
    numeric = float(magnitude)
    signed = -numeric if movement is EurostatGdpMovement.DOWN else numeric
    return EurostatGdpReleaseValueV1(
        economy_code="EA",
        reference_period=reference_period,
        measure=EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER,
        movement=movement,
        magnitude_lexical=magnitude,
        value=signed,
        evidence_id=entry.entry_id,
    )


class _ReleaseLandingParser(HTMLParser):
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
    entry: EurostatGdpInventoryEntryV1,
) -> tuple[str, str, str | None, tuple[str, ...]]:
    try:
        content = snapshot.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Eurostat GDP release landing is not UTF-8") from exc
    parser = _ReleaseLandingParser()
    parser.feed(content)
    parser.close()
    titles = tuple(
        dict.fromkeys(
            _required_text(item, "landing_title") for item in parser.og_titles
        )
    )
    if titles != (entry.source_title,):
        raise ValueError("Eurostat GDP Atom and landing titles differ")
    text = " ".join(parser.text)
    page_dates: set[str] = set()
    for date_match in _LANDING_DATE_RE.finditer(text):
        month = _MONTHS.get(date_match.group("month").casefold())
        if month is None:
            raise ValueError("Eurostat GDP landing month is unsupported")
        try:
            page_dates.add(
                date(
                    int(date_match.group("year")),
                    month,
                    int(date_match.group("day")),
                ).isoformat()
            )
        except ValueError as exc:
            raise ValueError("Eurostat GDP landing date is invalid") from exc
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
            "Eurostat GDP release landing has no unique publication time"
        )
    published_at = published_values[0] if published_values else None
    if published_at is not None:
        page_dates.add(
            datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            .date()
            .isoformat()
        )
    if len(page_dates) != 1:
        raise ValueError(
            "Eurostat GDP release landing has no unique release date"
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
            "Eurostat GDP landing exposes multiple English release documents"
        )
    return titles[0], page_date, published_at, tuple(sorted(document_uris))


class _VisibleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.values: list[str] = []

    def handle_data(self, data: str) -> None:
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
                    "Eurostat GDP release PDF has too many unreadable pages"
                )
            text = " ".join(page_text)
        except (PdfReadError, ValueError) as exc:
            raise ValueError(
                "Eurostat GDP release PDF text extraction failed"
            ) from exc
    else:
        raise ValueError("Eurostat GDP release value source format differs")
    normalized = _SPACE_RE.sub(" ", text.replace("\u2212", "-")).strip()
    if not normalized:
        raise ValueError("Eurostat GDP release value source has no text")
    return normalized


_QUARTER_NUMBER_BY_NAME: Final[Mapping[str, int]] = {
    "first": 1,
    "1st": 1,
    "second": 2,
    "2nd": 2,
    "third": 3,
    "3rd": 3,
    "fourth": 4,
    "4th": 4,
}
_REFERENCE_QUARTER_RE = re.compile(
    r"\b(?P<quarter>first|second|third|fourth|1st|2nd|3rd|4th)\s+"
    r"quarter(?:\s+of)?\s+(?P<year>20\d{2})\b",
    re.IGNORECASE,
)


def _reference_period_from_publication(
    entry: EurostatGdpInventoryEntryV1,
    landing_snapshot: OfficialRawSnapshotV1,
    document_snapshot: OfficialRawSnapshotV1 | None,
) -> tuple[str, int]:
    snapshots = (
        (document_snapshot, landing_snapshot)
        if document_snapshot is not None
        else (landing_snapshot,)
    )
    for snapshot in snapshots:
        text = _release_source_text(snapshot)
        for match in _REFERENCE_QUARTER_RE.finditer(text):
            quarter = _QUARTER_NUMBER_BY_NAME[match.group("quarter").casefold()]
            reference_period = f"{match.group('year')}-Q{quarter}"
            try:
                lag = _release_lag(entry.release_date, reference_period)
            except ValueError:
                continue
            return reference_period, lag
    raise ValueError(
        "Eurostat GDP publication omits a qualified reference quarter for "
        f"{entry.product_code}"
    )


_GDP_TABLE_START_RE = re.compile(
    r"\bGrowth rates? of GDP(?: in volume)?\b",
    re.IGNORECASE,
)
_GDP_TABLE_FALLBACK_START_RE = re.compile(
    r"\bGDP growth rates?\b",
    re.IGNORECASE,
)
_GDP_TABLE_END_RE = re.compile(
    r"\b(?:Growth rates? of employment|The EU Member States)\b|"
    r":\s*Data not available",
    re.IGNORECASE,
)
_TABLE_VALUE_TOKEN = r"(?:\((?:[+-]?\d+(?:\.\d+)?|:)\)|[+-]?\d+(?:\.\d+)?|:)"
_TABLE_ROW_LABELS: Final[Mapping[str, tuple[str, ...]]] = {
    "EA": ("Euro area", "Euro-zone", "EA"),
    "DE": ("Germany", "D"),
    "FR": ("France", "F"),
}
_EA_COMPOSITION_RE = re.compile(
    r"\bEuro[ -](?:area|zone)\s*\d*\s*\((?P<label>EA\d{1,2})\)",
    re.IGNORECASE,
)


def _table_published_values(
    snapshot: OfficialRawSnapshotV1,
    artifact: EurostatGdpArtifactV1,
    reference_period: str,
) -> tuple[EurostatGdpPublishedValueV1, ...]:
    text = _release_source_text(snapshot)
    start_match = _GDP_TABLE_START_RE.search(text)
    if start_match is None:
        start_match = _GDP_TABLE_FALLBACK_START_RE.search(text)
    if start_match is None:
        return ()
    remainder = text[start_match.start() :]
    end_match = _GDP_TABLE_END_RE.search(remainder)
    section = (
        remainder[: end_match.end()] if end_match is not None else remainder
    )
    section = re.sub(r"(?<=\d)(?=[+-]\d)", " ", section)
    preface = text[: start_match.start()]
    ea_composition_labels = tuple(
        dict.fromkeys(
            match.group("label").upper()
            for match in _EA_COMPOSITION_RE.finditer(preface)
        )
    )
    selected_composition = _ea_composition_for_period(reference_period)
    if (
        ea_composition_labels
        and selected_composition not in ea_composition_labels
    ):
        raise ValueError(
            "Eurostat GDP stated EA composition differs for "
            f"{artifact.product_code}: expected={selected_composition}, "
            f"observed={ea_composition_labels}"
        )
    section = re.sub(
        rf"\b{re.escape(selected_composition)}\d(?=\s)",
        selected_composition,
        section,
    )
    year = reference_period[:4]
    quarter = f"Q{reference_period[-1]}"
    results: list[EurostatGdpPublishedValueV1] = []
    expected_width: int | None = None
    for economy, labels in _TABLE_ROW_LABELS.items():
        if economy == "EA":
            composition_number = selected_composition.removeprefix("EA")
            composition_labels = (
                selected_composition,
                f"Euro area {composition_number}",
                f"Euro-zone {composition_number}",
            )
            composition_pattern = "|".join(
                re.escape(item)
                for item in sorted(composition_labels, key=len, reverse=True)
            )
            if re.search(
                rf"(?<![\w-])(?:{composition_pattern})(?![\w-])\s+"
                rf"{_TABLE_VALUE_TOKEN}",
                section,
                re.IGNORECASE,
            ):
                labels = composition_labels
            else:
                labels = (*labels, selected_composition)
        alternatives = "|".join(
            re.escape(item) for item in sorted(labels, key=len, reverse=True)
        )
        row_pattern = re.compile(
            rf"(?<![\w-])(?P<label>{alternatives})(?![\w-])(?:\*+)?\s+"
            rf"(?P<values>{_TABLE_VALUE_TOKEN}(?:\s+{_TABLE_VALUE_TOKEN}){{1,17}})",
            re.IGNORECASE,
        )
        matches = tuple(row_pattern.finditer(section))
        if len(matches) > 1:
            row_signatures = {
                (
                    match.group("label").casefold(),
                    tuple(match.group("values").split()),
                )
                for match in matches
            }
            if len(row_signatures) > 1:
                raise ValueError(
                    "Eurostat GDP publication repeats a conflicting GDP table "
                    f"row for {artifact.product_code} {economy}: "
                    f"matches={len(matches)}"
                )
        if not matches:
            continue
        match = matches[0]
        header = section[: match.start()]
        if year not in header or quarter.casefold() not in header.casefold():
            raise ValueError(
                "Eurostat GDP table omits its reference-period header for "
                f"{artifact.product_code}: {reference_period}"
            )
        raw_values = match.group("values").split()
        if expected_width is None:
            expected_width = len(raw_values)
        if len(raw_values) < expected_width or any(
            item != ":" for item in raw_values[expected_width:]
        ):
            raise ValueError(
                "Eurostat GDP publication table row width differs for "
                f"{artifact.product_code} {economy}: "
                f"expected={expected_width}, observed={len(raw_values)}"
            )
        values = raw_values[:expected_width]
        if len(values) < 4 or len(values) % 2 or len(values) > 18:
            raise ValueError("Eurostat GDP publication table width differs")
        parenthesized = tuple(
            index for index, item in enumerate(values) if item.startswith("(")
        )
        if parenthesized:
            expected_parenthesized = (len(values) // 2 - 1, len(values) - 1)
            if parenthesized != expected_parenthesized:
                raise ValueError(
                    "Eurostat GDP previous-release table columns differ"
                )
            period_count = len(values) // 2 - 1
            qoq_lexical = values[parenthesized[0] - 1]
            yoy_lexical = values[parenthesized[1] - 1]
        else:
            period_count = len(values) // 2
            qoq_lexical = values[period_count - 1]
            yoy_lexical = values[-1]
        selected = (
            (EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER, qoq_lexical),
            (EurostatGdpGrowthMeasure.YEAR_OVER_YEAR, yoy_lexical),
        )
        for measure, lexical in selected:
            if lexical == ":":
                continue
            results.append(
                EurostatGdpPublishedValueV1(
                    economy_code=economy,
                    reference_period=reference_period,
                    measure=measure,
                    value_lexical=lexical,
                    value=float(lexical),
                    evidence_artifact_sha256=artifact.content_sha256,
                    evidence_locator=(
                        "GDP growth table; "
                        f"{match.group('label')} row; latest of "
                        f"{period_count} quarterly columns; {measure.value}"
                    ),
                )
            )
    return tuple(results)


def _published_values(
    landing_snapshot: OfficialRawSnapshotV1,
    landing_artifact: EurostatGdpArtifactV1,
    document_snapshot: OfficialRawSnapshotV1 | None,
    document_artifact: EurostatGdpArtifactV1 | None,
    reference_period: str,
    headline: EurostatGdpReleaseValueV1,
) -> tuple[EurostatGdpPublishedValueV1, ...]:
    parsed: tuple[EurostatGdpPublishedValueV1, ...] = ()
    for snapshot, artifact in (
        (landing_snapshot, landing_artifact),
        (document_snapshot, document_artifact),
    ):
        if snapshot is None or artifact is None:
            continue
        parsed = _table_published_values(snapshot, artifact, reference_period)
        if parsed:
            break
    if not parsed:
        raise ValueError(
            "Eurostat GDP publication has no GDP growth table for "
            f"{landing_artifact.product_code}"
        )
    values = list(parsed)
    headline_match = next(
        (
            item
            for item in values
            if item.economy_code == "EA"
            and item.measure is EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER
        ),
        None,
    )
    if headline_match is None:
        raise ValueError(
            "Eurostat GDP publication table omits EA q/q GDP for "
            f"{landing_artifact.product_code}"
        )
    if headline_match.value != headline.value:
        raise ValueError(
            "Eurostat GDP headline and publication table differ for "
            f"{landing_artifact.product_code}: headline={headline.value}, "
            f"table={headline_match.value}"
        )
    measure_order = {
        EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER: 0,
        EurostatGdpGrowthMeasure.YEAR_OVER_YEAR: 1,
    }
    values.sort(
        key=lambda item: (
            _RELEASE_ECONOMY_ORDER[item.economy_code],
            measure_order[item.measure],
        )
    )
    return tuple(values)


@dataclass(frozen=True, slots=True)
class EurostatGdpReleaseV1:
    """One source-dated GDP estimate with explicit sequence semantics."""

    inventory_entry: EurostatGdpInventoryEntryV1
    reference_period: str
    stage: EurostatGdpReleaseStage
    days_after_period_end: int
    page_release_date: str
    page_release_date_offset_days: int
    published_at: str | None
    linked_document_uris: tuple[str, ...]
    artifact: EurostatGdpArtifactV1
    document_artifact: EurostatGdpArtifactV1 | None
    headline_value: EurostatGdpReleaseValueV1
    published_values: tuple[EurostatGdpPublishedValueV1, ...]
    release_id: str = ""
    schema_version: str = EUROSTAT_GDP_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_GDP_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat GDP release schema")
        reference = _quarter(self.reference_period, "reference_period")
        stage = EurostatGdpReleaseStage.from_value(self.stage)
        expected_lag = _release_lag(
            self.inventory_entry.release_date, reference
        )
        if self.days_after_period_end != expected_lag:
            raise ValueError("Eurostat GDP release period or lag differs")
        page_date = _iso_date(self.page_release_date, "page_release_date")
        offset = (
            date.fromisoformat(page_date)
            - date.fromisoformat(self.inventory_entry.release_date)
        ).days
        if self.page_release_date_offset_days != offset or offset not in {
            -1,
            0,
            1,
        }:
            raise ValueError(
                "Eurostat GDP landing date offset differs for "
                f"{self.inventory_entry.product_code}: offset={offset}"
            )
        published_at = _optional_text(self.published_at, "published_at")
        if published_at is not None:
            published_at = _iso_datetime(published_at, "published_at")
            published_date = datetime.fromisoformat(
                published_at.replace("Z", "+00:00")
            ).date()
            if published_date.isoformat() != page_date:
                raise ValueError(
                    "Eurostat GDP publication timestamp date differs"
                )
        links = tuple(
            _https_uri(item, "linked_document_uri")
            for item in self.linked_document_uris
        )
        if links != tuple(sorted(set(links))):
            raise ValueError(
                "Eurostat GDP release document links are not canonical"
            )
        if len(links) > 1:
            raise ValueError(
                "Eurostat GDP release has multiple English documents"
            )
        if any(
            _product_code(item) != self.inventory_entry.product_code
            for item in links
        ):
            raise ValueError(
                "Eurostat GDP release document product code differs"
            )
        if self.artifact.role is not EurostatGdpArtifactRole.RELEASE_HTML:
            raise ValueError("Eurostat GDP release artifact role differs")
        if self.artifact.product_code != self.inventory_entry.product_code:
            raise ValueError(
                "Eurostat GDP release artifact product code differs"
            )
        document = self.document_artifact
        if (document is None) != (not links):
            raise ValueError("Eurostat GDP release document artifact differs")
        if document is not None and (
            document.role
            not in {
                EurostatGdpArtifactRole.RELEASE_DOCUMENT_HTML,
                EurostatGdpArtifactRole.RELEASE_DOCUMENT_PDF,
            }
            or document.request_uri != links[0]
            or document.product_code != self.inventory_entry.product_code
        ):
            raise ValueError("Eurostat GDP release document identity differs")
        value = self.headline_value
        if (
            value.reference_period != reference
            or value.evidence_id != self.inventory_entry.entry_id
        ):
            raise ValueError("Eurostat GDP release headline occurrence differs")
        published_values = tuple(self.published_values)
        if not 2 <= len(published_values) <= 6:
            raise ValueError("Eurostat GDP published-value count differs")
        keys = [item.key for item in published_values]
        if len(keys) != len(set(keys)):
            raise ValueError("Eurostat GDP release repeats a published value")
        measure_order = {
            EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER: 0,
            EurostatGdpGrowthMeasure.YEAR_OVER_YEAR: 1,
        }
        if list(published_values) != sorted(
            published_values,
            key=lambda item: (
                _RELEASE_ECONOMY_ORDER[item.economy_code],
                measure_order[item.measure],
            ),
        ):
            raise ValueError("Eurostat GDP published values are not canonical")
        evidence_digests = {self.artifact.content_sha256}
        if document is not None:
            evidence_digests.add(document.content_sha256)
        if any(
            item.reference_period != reference
            or item.evidence_artifact_sha256 not in evidence_digests
            for item in published_values
        ):
            raise ValueError("Eurostat GDP published-value evidence differs")
        headline_published = next(
            (
                item
                for item in published_values
                if item.economy_code == "EA"
                and item.measure
                is EurostatGdpGrowthMeasure.QUARTER_OVER_QUARTER
            ),
            None,
        )
        if (
            headline_published is None
            or headline_published.value != value.value
        ):
            raise ValueError("Eurostat GDP headline published value differs")
        if not any(
            item.economy_code == "EA"
            and item.measure is EurostatGdpGrowthMeasure.YEAR_OVER_YEAR
            for item in published_values
        ):
            raise ValueError("Eurostat GDP publication table omits EA y/y GDP")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "stage", stage)
        object.__setattr__(self, "page_release_date", page_date)
        object.__setattr__(self, "published_at", published_at)
        object.__setattr__(self, "linked_document_uris", links)
        object.__setattr__(self, "published_values", published_values)
        expected = _stable_id("eurostat-gdp-release", self.identity_payload())
        if self.release_id and self.release_id != expected:
            raise ValueError("Eurostat GDP release identity differs")
        object.__setattr__(self, "release_id", expected)

    @property
    def occurrence_key(self) -> tuple[str, str]:
        return self.reference_period, self.stage.value

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "inventory_entry": self.inventory_entry.to_dict(),
            "reference_period": self.reference_period,
            "stage": self.stage.value,
            "days_after_period_end": self.days_after_period_end,
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatGdpReleaseV1:
        return cls(
            inventory_entry=EurostatGdpInventoryEntryV1.from_dict(
                _mapping(data.get("inventory_entry"), "inventory_entry")
            ),
            reference_period=str(data.get("reference_period", "")),
            stage=EurostatGdpReleaseStage.from_value(
                str(data.get("stage", ""))
            ),
            days_after_period_end=cast(int, data.get("days_after_period_end")),
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
            artifact=EurostatGdpArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            document_artifact=(
                EurostatGdpArtifactV1.from_dict(
                    _mapping(data.get("document_artifact"), "document_artifact")
                )
                if data.get("document_artifact") is not None
                else None
            ),
            headline_value=EurostatGdpReleaseValueV1.from_dict(
                _mapping(data.get("headline_value"), "headline_value")
            ),
            published_values=tuple(
                EurostatGdpPublishedValueV1.from_dict(
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
    releases: Sequence[EurostatGdpReleaseV1],
) -> tuple[int, int]:
    series: dict[tuple[str, str, str], list[float]] = {}
    for release in releases:
        for value in release.published_values:
            key = (
                value.reference_period,
                value.economy_code,
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


@dataclass(frozen=True, slots=True)
class EurostatGdpArchiveManifestV1:
    """Compact, replayable receipt for the official quarterly-GDP corpus."""

    as_of_date: str
    registry_id: str
    source_id: str
    inventory: EurostatGdpInventoryV1
    current_dataset: EurostatGdpDatasetV1
    releases: tuple[EurostatGdpReleaseV1, ...]
    searchable_release_gap_reference_periods: tuple[str, ...]
    preliminary_flash_count: int
    flash_count: int
    first_estimate_count: int
    second_estimate_count: int
    third_estimate_count: int
    regular_estimate_count: int
    page_date_offset_count: int
    exact_publication_time_count: int
    release_document_count: int
    published_value_count: int
    revision_comparison_count: int
    changed_revision_count: int
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    manifest_id: str = ""
    schema_version: str = EUROSTAT_GDP_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_GDP_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat GDP manifest schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        if as_of != EUROSTAT_GDP_AS_OF_DATE:
            raise ValueError("Eurostat GDP archive boundary differs")
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError("Eurostat GDP registry identity is invalid")
        if not source_id.startswith("official-source:sha256:"):
            raise ValueError("Eurostat GDP source identity is invalid")
        releases = tuple(self.releases)
        if len(releases) != EUROSTAT_GDP_RELEASE_COUNT:
            raise ValueError("Eurostat GDP manifest release count differs")
        if list(releases) != sorted(
            releases,
            key=lambda item: (
                item.inventory_entry.release_date,
                item.inventory_entry.product_code,
            ),
        ):
            raise ValueError("Eurostat GDP releases are not canonical")
        if [item.inventory_entry for item in releases] != list(
            self.inventory.entries
        ):
            raise ValueError(
                "Eurostat GDP release and inventory entries differ"
            )
        period_lags: dict[str, list[int]] = {}
        for item in releases:
            period_lags.setdefault(item.reference_period, []).append(
                item.days_after_period_end
            )
        if any(
            item.stage
            is not _release_stage(
                item.inventory_entry.release_date,
                item.days_after_period_end,
                period_lags[item.reference_period],
            )
            for item in releases
        ):
            raise ValueError("Eurostat GDP release-stage evidence differs")
        if any(
            item.headline_value
            != _headline_value(item.inventory_entry, item.reference_period)
            for item in releases
        ):
            raise ValueError("Eurostat GDP release headline evidence differs")
        occurrences = [item.occurrence_key for item in releases]
        if len(occurrences) != len(set(occurrences)):
            raise ValueError("Eurostat GDP releases repeat a stage occurrence")
        if any(item.inventory_entry.release_date > as_of for item in releases):
            raise ValueError("Eurostat GDP release exceeds archive boundary")
        if self.current_dataset.updated_at[:10] > as_of:
            raise ValueError("Eurostat GDP dataset exceeds archive boundary")
        first_period = min(item.reference_period for item in releases)
        expected_gaps = _quarter_range(
            self.current_dataset.time_start, _quarter_shift(first_period, -1)
        )
        gaps = tuple(
            _quarter(item, "searchable_release_gap_reference_period")
            for item in self.searchable_release_gap_reference_periods
        )
        if gaps != expected_gaps:
            raise ValueError(
                "Eurostat GDP searchable-release gap inventory differs"
            )
        stage_counts = {
            stage: sum(item.stage is stage for item in releases)
            for stage in EurostatGdpReleaseStage
        }
        observed_counts = {
            EurostatGdpReleaseStage.PRELIMINARY_FLASH: self.preliminary_flash_count,
            EurostatGdpReleaseStage.FLASH: self.flash_count,
            EurostatGdpReleaseStage.FIRST_ESTIMATE: self.first_estimate_count,
            EurostatGdpReleaseStage.SECOND_ESTIMATE: self.second_estimate_count,
            EurostatGdpReleaseStage.THIRD_ESTIMATE: self.third_estimate_count,
            EurostatGdpReleaseStage.REGULAR_ESTIMATE: self.regular_estimate_count,
        }
        if stage_counts != observed_counts:
            raise ValueError("Eurostat GDP release-stage counts differ")
        expected_offsets = sum(
            item.page_release_date_offset_days != 0 for item in releases
        )
        if self.page_date_offset_count != expected_offsets:
            raise ValueError("Eurostat GDP page-date offset count differs")
        expected_exact_times = sum(
            item.published_at is not None for item in releases
        )
        if self.exact_publication_time_count != expected_exact_times:
            raise ValueError(
                "Eurostat GDP exact publication-time count differs"
            )
        document_artifacts = tuple(
            item.document_artifact
            for item in releases
            if item.document_artifact is not None
        )
        if len(document_artifacts) != EUROSTAT_GDP_DOCUMENT_COUNT:
            raise ValueError("Eurostat GDP English document count differs")
        if self.release_document_count != len(document_artifacts):
            raise ValueError("Eurostat GDP release-document count differs")
        expected_value_count = sum(
            len(item.published_values) for item in releases
        )
        if self.published_value_count != expected_value_count:
            raise ValueError("Eurostat GDP published-value count differs")
        revision_counts = _published_revision_counts(releases)
        if revision_counts != (
            self.revision_comparison_count,
            self.changed_revision_count,
        ):
            raise ValueError("Eurostat GDP revision counts differ")
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
                MAX_EUROSTAT_GDP_ARTIFACTS,
            ),
            _bounded_int(
                self.unique_content_sha256_count,
                "unique_content_sha256_count",
                MAX_EUROSTAT_GDP_ARTIFACTS,
            ),
            _bounded_int(
                self.total_content_bytes,
                "total_content_bytes",
                MAX_EUROSTAT_GDP_TOTAL_BYTES,
            ),
        )
        if counts != (
            expected_artifact_count,
            expected_unique_count,
            expected_bytes,
        ):
            raise ValueError("Eurostat GDP artifact totals differ")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "releases", releases)
        object.__setattr__(
            self, "searchable_release_gap_reference_periods", gaps
        )
        expected = _stable_id(
            "eurostat-gdp-archive-manifest", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("Eurostat GDP manifest identity differs")
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
            "preliminary_flash_count": self.preliminary_flash_count,
            "flash_count": self.flash_count,
            "first_estimate_count": self.first_estimate_count,
            "second_estimate_count": self.second_estimate_count,
            "third_estimate_count": self.third_estimate_count,
            "regular_estimate_count": self.regular_estimate_count,
            "page_date_offset_count": self.page_date_offset_count,
            "exact_publication_time_count": self.exact_publication_time_count,
            "release_document_count": self.release_document_count,
            "published_value_count": self.published_value_count,
            "revision_comparison_count": self.revision_comparison_count,
            "changed_revision_count": self.changed_revision_count,
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatGdpArchiveManifestV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            inventory=EurostatGdpInventoryV1.from_dict(
                _mapping(data.get("inventory"), "inventory")
            ),
            current_dataset=EurostatGdpDatasetV1.from_dict(
                _mapping(data.get("current_dataset"), "current_dataset")
            ),
            releases=tuple(
                EurostatGdpReleaseV1.from_dict(_mapping(item, "release"))
                for item in _sequence(data.get("releases"), "releases")
            ),
            searchable_release_gap_reference_periods=tuple(
                str(item)
                for item in _sequence(
                    data.get("searchable_release_gap_reference_periods"),
                    "searchable_release_gap_reference_periods",
                )
            ),
            preliminary_flash_count=cast(
                int, data.get("preliminary_flash_count")
            ),
            flash_count=cast(int, data.get("flash_count")),
            first_estimate_count=cast(int, data.get("first_estimate_count")),
            second_estimate_count=cast(int, data.get("second_estimate_count")),
            third_estimate_count=cast(int, data.get("third_estimate_count")),
            regular_estimate_count=cast(
                int, data.get("regular_estimate_count")
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
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, value: str) -> EurostatGdpArchiveManifestV1:
        try:
            data = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("Eurostat GDP manifest is invalid JSON") from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_eurostat_gdp_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    dataset_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatGdpArchiveManifestV1:
    """Build the GDP receipt from exact caller-retained Eurostat bytes."""
    source = registry.source(EUROSTAT_GDP_SOURCE_KEY)
    if (
        source.parser_id != EUROSTAT_GDP_PARSER_ID
        or source.parser_version != EUROSTAT_GDP_PARSER_VERSION
    ):
        raise ValueError("Eurostat GDP registry parser identity differs")
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
        raise ValueError("Eurostat GDP snapshot source identity differs")
    inventory = parse_eurostat_gdp_inventory(
        search_values, as_of_date=as_of_date
    )
    dataset = parse_eurostat_gdp_dataset(dataset_snapshot)
    snapshots = release_values
    if len(snapshots) != EUROSTAT_GDP_RELEASE_COUNT:
        raise ValueError("Eurostat GDP release snapshot count differs")
    by_code: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        code = _product_code(snapshot.request.uri)
        if code in by_code:
            raise ValueError(
                "Eurostat GDP release corpus repeats a product code"
            )
        by_code[code] = snapshot
    if set(by_code) != {item.product_code for item in inventory.entries}:
        raise ValueError("Eurostat GDP release corpus is incomplete")
    if len(document_values) != EUROSTAT_GDP_DOCUMENT_COUNT:
        raise ValueError("Eurostat GDP document snapshot count differs")
    documents_by_uri: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in document_values:
        if snapshot.request.uri in documents_by_uri:
            raise ValueError("Eurostat GDP document corpus repeats a URI")
        documents_by_uri[snapshot.request.uri] = snapshot
    used_document_uris: set[str] = set()
    parsed: list[
        tuple[
            EurostatGdpInventoryEntryV1,
            EurostatGdpArtifactV1,
            EurostatGdpArtifactV1 | None,
            str,
            str | None,
            str,
            int,
            tuple[str, ...],
        ]
    ] = []
    period_lags: dict[str, list[int]] = {}
    for entry in inventory.entries:
        snapshot = by_code[entry.product_code]
        if snapshot.request.uri != entry.source_uri:
            raise ValueError("Eurostat GDP release request identity differs")
        artifact = _artifact_from_snapshot(
            snapshot,
            EurostatGdpArtifactRole.RELEASE_HTML,
            product_code=entry.product_code,
        )
        _, page_date, published_at, document_uris = _parse_release_landing(
            snapshot, entry
        )
        document_artifact: EurostatGdpArtifactV1 | None = None
        document_snapshot: OfficialRawSnapshotV1 | None = None
        if document_uris:
            document_uri = document_uris[0]
            try:
                document_snapshot = documents_by_uri[document_uri]
            except KeyError as exc:
                raise ValueError(
                    "Eurostat GDP document corpus is incomplete"
                ) from exc
            used_document_uris.add(document_uri)
            document_role = {
                OfficialSourceFormat.HTML: (
                    EurostatGdpArtifactRole.RELEASE_DOCUMENT_HTML
                ),
                OfficialSourceFormat.PDF: (
                    EurostatGdpArtifactRole.RELEASE_DOCUMENT_PDF
                ),
            }.get(document_snapshot.request.source_format)
            if document_role is None:
                raise ValueError("Eurostat GDP document format differs")
            document_artifact = _artifact_from_snapshot(
                document_snapshot,
                document_role,
                product_code=entry.product_code,
            )
        reference_period, lag = _reference_period_from_publication(
            entry, snapshot, document_snapshot
        )
        period_lags.setdefault(reference_period, []).append(lag)
        parsed.append(
            (
                entry,
                artifact,
                document_artifact,
                page_date,
                published_at,
                reference_period,
                lag,
                document_uris,
            )
        )
    if used_document_uris != set(documents_by_uri):
        raise ValueError(
            "Eurostat GDP document corpus has unexpected artifacts"
        )
    releases: list[EurostatGdpReleaseV1] = []
    for (
        entry,
        artifact,
        document_artifact,
        page_date,
        published_at,
        reference_period,
        lag,
        document_uris,
    ) in parsed:
        expected_lag = _release_lag(entry.release_date, reference_period)
        if lag != expected_lag:
            raise ValueError("Eurostat GDP release lag changed during parsing")
        stage = _release_stage(
            entry.release_date, lag, period_lags[reference_period]
        )
        headline = _headline_value(entry, reference_period)
        document_snapshot = (
            documents_by_uri[document_uris[0]] if document_uris else None
        )
        releases.append(
            EurostatGdpReleaseV1(
                inventory_entry=entry,
                reference_period=reference_period,
                stage=stage,
                days_after_period_end=lag,
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
    stage_counts = {
        stage: sum(item.stage is stage for item in releases)
        for stage in EurostatGdpReleaseStage
    }
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
    first_period = min(item.reference_period for item in releases)
    revision_comparison_count, changed_revision_count = (
        _published_revision_counts(releases)
    )
    return EurostatGdpArchiveManifestV1(
        as_of_date=as_of_date,
        registry_id=registry.registry_id,
        source_id=source.source_id,
        inventory=inventory,
        current_dataset=dataset,
        releases=tuple(releases),
        searchable_release_gap_reference_periods=_quarter_range(
            dataset.time_start, _quarter_shift(first_period, -1)
        ),
        preliminary_flash_count=stage_counts[
            EurostatGdpReleaseStage.PRELIMINARY_FLASH
        ],
        flash_count=stage_counts[EurostatGdpReleaseStage.FLASH],
        first_estimate_count=stage_counts[
            EurostatGdpReleaseStage.FIRST_ESTIMATE
        ],
        second_estimate_count=stage_counts[
            EurostatGdpReleaseStage.SECOND_ESTIMATE
        ],
        third_estimate_count=stage_counts[
            EurostatGdpReleaseStage.THIRD_ESTIMATE
        ],
        regular_estimate_count=stage_counts[
            EurostatGdpReleaseStage.REGULAR_ESTIMATE
        ],
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
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
    )


def replay_eurostat_gdp_archive(
    registry: OfficialSourceRegistryV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    dataset_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Sequence[OfficialRawSnapshotV1],
    expected_manifest: EurostatGdpArchiveManifestV1,
) -> EurostatGdpArchiveManifestV1:
    """Rebuild the GDP receipt and require byte-for-byte identity."""
    rebuilt = build_eurostat_gdp_archive_manifest(
        registry,
        search_snapshots,
        dataset_snapshot,
        release_snapshots,
        document_snapshots,
        as_of_date=expected_manifest.as_of_date,
    )
    if rebuilt.to_json() != expected_manifest.to_json():
        raise ValueError("Eurostat GDP replay differs from packaged manifest")
    return rebuilt


def packaged_eurostat_gdp_manifest_path() -> Path:
    """Return the installed GDP archive-manifest path."""
    return (
        Path(__file__).resolve().parent
        / "assets"
        / "eurostat_gdp_archive_v1.json"
    )


def load_packaged_eurostat_gdp_archive_manifest() -> (
    EurostatGdpArchiveManifestV1
):
    """Load and validate the installed GDP archive manifest."""
    path = packaged_eurostat_gdp_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError("packaged Eurostat GDP manifest exceeds size bound")
    manifest = EurostatGdpArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_GDP_SOURCE_KEY)
    if (
        manifest.registry_id != registry.registry_id
        or manifest.source_id != source.source_id
    ):
        raise ValueError("packaged Eurostat GDP registry binding differs")
    return manifest


__all__ = [
    "EUROSTAT_GDP_ARTIFACT_SCHEMA_VERSION",
    "EUROSTAT_GDP_AS_OF_DATE",
    "EUROSTAT_GDP_DATASET_SCHEMA_VERSION",
    "EUROSTAT_GDP_DATASET_URI",
    "EUROSTAT_GDP_DOCUMENT_COUNT",
    "EUROSTAT_GDP_EXCLUSION_SCHEMA_VERSION",
    "EUROSTAT_GDP_FIRST_SEARCH_RELEASE_DATE",
    "EUROSTAT_GDP_INVENTORY_ENTRY_SCHEMA_VERSION",
    "EUROSTAT_GDP_INVENTORY_SCHEMA_VERSION",
    "EUROSTAT_GDP_LATEST_PACKAGED_RELEASE_DATE",
    "EUROSTAT_GDP_MANIFEST_SCHEMA_VERSION",
    "EUROSTAT_GDP_MEASURE_KEY",
    "EUROSTAT_GDP_OBSERVATION_SCHEMA_VERSION",
    "EUROSTAT_GDP_PARSER_ID",
    "EUROSTAT_GDP_PARSER_VERSION",
    "EUROSTAT_GDP_PROGRAM_KEY",
    "EUROSTAT_GDP_PUBLISHED_VALUE_SCHEMA_VERSION",
    "EUROSTAT_GDP_RELEASE_COUNT",
    "EUROSTAT_GDP_RELEASE_SCHEMA_VERSION",
    "EUROSTAT_GDP_RELEASE_VALUE_SCHEMA_VERSION",
    "EUROSTAT_GDP_SEARCH_PAGE_COUNT",
    "EUROSTAT_GDP_SEARCH_RESULT_COUNT",
    "EUROSTAT_GDP_SEARCH_URI",
    "EUROSTAT_GDP_SOURCE_KEY",
    "EUROSTAT_GDP_SOURCE_TIMEZONE",
    "EurostatGdpArchiveManifestV1",
    "EurostatGdpArtifactRole",
    "EurostatGdpArtifactV1",
    "EurostatGdpDatasetV1",
    "EurostatGdpGrowthMeasure",
    "EurostatGdpIndexExclusionV1",
    "EurostatGdpInventoryEntryV1",
    "EurostatGdpInventoryV1",
    "EurostatGdpMovement",
    "EurostatGdpObservationV1",
    "EurostatGdpPublishedValueV1",
    "EurostatGdpReleaseStage",
    "EurostatGdpReleaseV1",
    "EurostatGdpReleaseValueV1",
    "build_eurostat_gdp_archive_manifest",
    "build_eurostat_gdp_dataset_request",
    "build_eurostat_gdp_document_requests",
    "build_eurostat_gdp_release_requests",
    "build_eurostat_gdp_search_requests",
    "load_packaged_eurostat_gdp_archive_manifest",
    "packaged_eurostat_gdp_manifest_path",
    "parse_eurostat_gdp_dataset",
    "parse_eurostat_gdp_inventory",
    "replay_eurostat_gdp_archive",
]
