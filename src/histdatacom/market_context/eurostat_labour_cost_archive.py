"""Deterministic qualification of Eurostat quarterly labour-cost releases.

Eurostat's searchable Euro-indicator feed is the release inventory, while
source-dated HTML/PDF products are the vintage evidence.  The current
``lc_lci_r2_q`` JSON-stat table is retained only as a revised-series cross-check;
it never substitutes for values as published in a release.
"""

from __future__ import annotations

import hashlib
import html
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
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

EUROSTAT_LABOUR_COST_SOURCE_KEY: Final = "ea.eurostat.labour-cost"
EUROSTAT_LABOUR_COST_PROGRAM_KEY: Final = "ea.eurostat.quarterly-labour-cost"
EUROSTAT_LABOUR_COST_MEASURE_KEY: Final = "labour-cost-index-annual-change"
EUROSTAT_LABOUR_COST_PARSER_ID: Final = "official.eurostat-labour-cost.v1"
EUROSTAT_LABOUR_COST_PARSER_VERSION: Final = "1"
EUROSTAT_LABOUR_COST_SOURCE_TIMEZONE: Final = "Europe/Brussels"
EUROSTAT_LABOUR_COST_AS_OF_DATE: Final = "2026-09-18"
EUROSTAT_LABOUR_COST_FIRST_SEARCH_RELEASE_DATE: Final = "2002-01-09"
EUROSTAT_LABOUR_COST_LATEST_PACKAGED_RELEASE_DATE: Final = "2026-09-16"
EUROSTAT_LABOUR_COST_FIRST_REFERENCE_PERIOD: Final = "2001-Q3"
EUROSTAT_LABOUR_COST_LATEST_REFERENCE_PERIOD: Final = "2026-Q2"
EUROSTAT_LABOUR_COST_ACTIVITY_SCOPE_BREAK_REFERENCE_PERIOD: Final = "2012-Q2"
EUROSTAT_LABOUR_COST_INTERNAL_GAP_REFERENCE_PERIODS: Final = ()
EUROSTAT_LABOUR_COST_PAGE_DATE_OFFSET_EXCEPTIONS: Final[dict[str, int]] = {
    "3-09012002-ap": -1,
    "3-22032002-ap": -1,
    "3-26062002-ap": -1,
    "3-25092002-ap": -1,
    "3-16122002-ap": -1,
    "3-19032003-bp": -1,
    "3-19062003-bp": -1,
    "3-19092003-ap": -1,
    "3-18122003-ap": -1,
    "3-18032004-ap": -1,
    "3-16062004-bp": -1,
}
EUROSTAT_LABOUR_COST_REFERENCE_PERIOD_INFERENCES: Final[dict[str, str]] = {}
EUROSTAT_LABOUR_COST_PERIOD_HEADER_CORRECTIONS: Final[
    dict[str, tuple[tuple[str, ...], tuple[str, ...]]]
] = {}
EUROSTAT_LABOUR_COST_SEARCH_PAGE_COUNT: Final = 2
EUROSTAT_LABOUR_COST_SEARCH_RESULT_COUNT: Final = 159
EUROSTAT_LABOUR_COST_SEARCH_UNIQUE_URI_COUNT: Final = 159
EUROSTAT_LABOUR_COST_TITLE_MATCH_COUNT: Final = 111
EUROSTAT_LABOUR_COST_PREFILTER_COUNT: Final = 100
EUROSTAT_LABOUR_COST_RELEASE_COUNT: Final = 100
EUROSTAT_LABOUR_COST_DOCUMENT_COUNT: Final = 89

EUROSTAT_LABOUR_COST_SEARCH_URI: Final = "https://ec.europa.eu/eurostat/search"
EUROSTAT_LABOUR_COST_PRODUCT_URI_TEMPLATE: Final = (
    "https://ec.europa.eu/eurostat/product?code={product_code}"
)
EUROSTAT_LABOUR_COST_DATASET_URI: Final = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "lc_lci_r2_q?lang=en&s_adj=CA&unit=PCH_SM&nace_r2=B-S&"
    "lcstruct=D1_D4_MD5&lcstruct=D11&lcstruct=D12_D4_MD5&"
    "geo=EA21&geo=DE&geo=FR&sinceTimePeriod=2000-Q1"
)

EUROSTAT_LABOUR_COST_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-labour-cost-artifact.v1"
)
EUROSTAT_LABOUR_COST_INVENTORY_ENTRY_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-labour-cost-inventory-entry.v1"
)
EUROSTAT_LABOUR_COST_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-labour-cost-exclusion.v1"
)
EUROSTAT_LABOUR_COST_INVENTORY_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-labour-cost-inventory.v1"
)
EUROSTAT_LABOUR_COST_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-labour-cost-observation.v1"
)
EUROSTAT_LABOUR_COST_DATASET_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-labour-cost-dataset.v1"
)
EUROSTAT_LABOUR_COST_RELEASE_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-labour-cost-release-value.v1"
)
EUROSTAT_LABOUR_COST_PUBLISHED_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-labour-cost-published-value.v1"
)
EUROSTAT_LABOUR_COST_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-labour-cost-release.v1"
)
EUROSTAT_LABOUR_COST_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-labour-cost-archive-manifest.v1"
)

MAX_EUROSTAT_LABOUR_COST_INDEX_PAGES: Final = 32
MAX_EUROSTAT_LABOUR_COST_SEARCH_ENTRIES: Final = 2_048
MAX_EUROSTAT_LABOUR_COST_RELEASES: Final = 512
MAX_EUROSTAT_LABOUR_COST_OBSERVATIONS: Final = 4_096
MAX_EUROSTAT_LABOUR_COST_ARTIFACTS: Final = 2_048
MAX_EUROSTAT_LABOUR_COST_TOTAL_BYTES: Final = 1_024_000_000
MAX_EUROSTAT_LABOUR_COST_TITLE_CHARS: Final = 512
MAX_EUROSTAT_LABOUR_COST_PUBLISHED_VALUES_PER_RELEASE: Final = 96

_SEARCH_PREFIX: Final = (
    "_estatsearchportlet_WAR_estatsearchportlet_INSTANCE_bHVzuvn1SZ8J_"
)
_ATOM_NAMESPACE: Final = "http://www.w3.org/2005/Atom"
_PRODUCT_CODE_RE = re.compile(
    r"(?P<prefix>[1-9])-(?P<day>\d{2})(?P<month>\d{2})(?P<year>0?\d{4})-"
    r"(?P<suffix>ap(?:\d+|_\d+)?|bp\d*|cp\d*)",
    re.IGNORECASE,
)
_PRODUCT_RELEASE_DATE_OVERRIDES: Final[dict[str, str]] = {}
_QUARTER_RE = re.compile(r"\d{4}-Q[1-4]")
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
        parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include an offset")
    return result


def _quarter(value: object, name: str) -> str:
    result = _required_text(value, name)
    if _QUARTER_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-Qn")
    return result


def _quarter_shift(value: str, offset: int) -> str:
    token = _quarter(value, "quarter")
    year = int(token[:4])
    quarter = int(token[-1])
    ordinal = year * 4 + quarter - 1 + offset
    return f"{ordinal // 4:04d}-Q{ordinal % 4 + 1}"


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
                "Eurostat labour cost product URI has no unique product code"
            )
        token = candidates[0]
    else:
        token = parsed.path if parsed.scheme else value
    token = token.casefold()
    path_tokens = token.strip("/").split("/")
    override = next(
        (
            candidate
            for candidate in _PRODUCT_RELEASE_DATE_OVERRIDES
            if candidate in path_tokens
        ),
        None,
    )
    if override is not None:
        return override
    match = _PRODUCT_CODE_RE.search(token)
    if match is None:
        raise ValueError("Eurostat labour cost product code is invalid")
    return match.group(0).casefold()


def _product_release_date(product_code: object) -> str:
    normalized = _product_code(product_code)
    override = _PRODUCT_RELEASE_DATE_OVERRIDES.get(normalized)
    if override is not None:
        return override
    match = _PRODUCT_CODE_RE.fullmatch(normalized)
    if match is None:  # pragma: no cover - guarded by _product_code
        raise ValueError("Eurostat labour cost product code is invalid")
    token = f"{int(match.group('year')):04d}-{match.group('month')}-{match.group('day')}"
    return _iso_date(token, "release_date")


def _inventory_prefilter(title: str) -> bool:
    normalized = _required_text(title, "title").casefold()
    prefixes = (
        "annual increase in labour costs ",
        "annual decrease in labour costs ",
        "annual growth in labour costs ",
        "annual growth in hourly labour costs ",
        "euro area and eu27 hourly labour costs ",
        "euro area hourly labour costs ",
        "euro area labour costs ",
        "euro area labour cost ",
        "euro-zone labour costs ",
        "euro-zone labour cost ",
        "euro-zone and eu15 labour costs ",
    )
    return normalized.startswith(prefixes) or normalized == (
        "hourly labour costs rose by 1.6% in both euro area and eu27"
    )


def _inventory_selected(title: str, product_code: str) -> bool:
    _product_code(product_code)
    return _inventory_prefilter(title)


def _exclusion_reason(title: str) -> str:
    normalized = _required_text(title, "title").casefold()
    if normalized.startswith(
        ("hourly labour costs ranged", "hourly labour costs in")
    ):
        return (
            "annual labour-cost-level article, not the quarterly index release"
        )
    if normalized.startswith("labour costs highest"):
        return (
            "sector-level labour-cost article, not the quarterly index release"
        )
    if "agricultural income" in normalized:
        return (
            "agricultural-income release, not the quarterly labour-cost index"
        )
    if "tax" in normalized or "vat" in normalized:
        return "tax-statistics release, not the quarterly labour-cost index"
    if "imbalance" in normalized:
        return "macro-imbalance summary, not the quarterly labour-cost index"
    if "business" in normalized:
        return (
            "business-statistics article, not the quarterly labour-cost index"
        )
    return "broad search match outside the quarterly labour-cost-index lineage"


class EurostatLabourCostArtifactRole(str, Enum):
    """Role of one exact official byte artifact."""

    SEARCH_INDEX = "search-index"
    CURRENT_DATASET = "current-dataset"
    RELEASE_HTML = "release-html"
    RELEASE_DOCUMENT_HTML = "release-document-html"
    RELEASE_DOCUMENT_PDF = "release-document-pdf"

    @classmethod
    def from_value(
        cls, value: str | EurostatLabourCostArtifactRole
    ) -> EurostatLabourCostArtifactRole:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat labour cost artifact role"
            ) from exc


class EurostatLabourCostMeasure(str, Enum):
    """Qualified labour-cost component carried by this archive."""

    TOTAL = "total-labour-cost-annual-percent"
    WAGES = "wages-and-salaries-annual-percent"
    OTHER = "other-labour-cost-annual-percent"

    @classmethod
    def from_value(
        cls, value: str | EurostatLabourCostMeasure
    ) -> EurostatLabourCostMeasure:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat labour cost measure"
            ) from exc


class EurostatLabourCostActivityScope(str, Enum):
    """Economic-activity aggregate represented by a source-dated release."""

    BUSINESS_ECONOMY = "business-economy"
    WHOLE_ECONOMY = "whole-economy"

    @classmethod
    def from_value(
        cls, value: str | EurostatLabourCostActivityScope
    ) -> EurostatLabourCostActivityScope:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat labour cost activity scope"
            ) from exc


@dataclass(frozen=True, slots=True)
class EurostatLabourCostArtifactV1:
    """Content-addressed official labour cost evidence artifact."""

    role: EurostatLabourCostArtifactRole
    request_uri: str
    resolved_uri: str
    source_format: OfficialSourceFormat
    content_length: int
    content_sha256: str
    page_number: int | None = None
    product_code: str | None = None
    schema_version: str = EUROSTAT_LABOUR_COST_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_LABOUR_COST_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat labour cost artifact schema")
        role = EurostatLabourCostArtifactRole.from_value(self.role)
        request_uri = _https_uri(self.request_uri, "request_uri")
        resolved_uri = _https_uri(self.resolved_uri, "resolved_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        expected_format = {
            EurostatLabourCostArtifactRole.SEARCH_INDEX: OfficialSourceFormat.ATOM,
            EurostatLabourCostArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
            EurostatLabourCostArtifactRole.RELEASE_HTML: OfficialSourceFormat.HTML,
            EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
            EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
        }[role]
        if source_format is not expected_format:
            raise ValueError(
                "Eurostat labour cost artifact role and format differ"
            )
        length = _bounded_int(
            self.content_length,
            "content_length",
            MAX_EUROSTAT_LABOUR_COST_TOTAL_BYTES,
        )
        if length < 1:
            raise ValueError("Eurostat labour cost artifact is empty")
        page_number = self.page_number
        product_code = self.product_code
        if role is EurostatLabourCostArtifactRole.SEARCH_INDEX:
            if page_number is None:
                raise ValueError(
                    "Eurostat labour cost search artifact needs a page number"
                )
            _bounded_int(
                page_number,
                "page_number",
                MAX_EUROSTAT_LABOUR_COST_INDEX_PAGES,
            )
            if product_code is not None:
                raise ValueError(
                    "Eurostat labour cost search artifact has a product code"
                )
        elif role is EurostatLabourCostArtifactRole.CURRENT_DATASET:
            if page_number is not None or product_code is not None:
                raise ValueError(
                    "Eurostat labour cost dataset artifact has release metadata"
                )
        else:
            if page_number is not None or product_code is None:
                raise ValueError(
                    "Eurostat labour cost release artifact metadata differs"
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatLabourCostArtifactV1:
        return cls(
            role=EurostatLabourCostArtifactRole.from_value(
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
class EurostatLabourCostInventoryEntryV1:
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
    schema_version: str = EUROSTAT_LABOUR_COST_INVENTORY_ENTRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_LABOUR_COST_INVENTORY_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat labour cost inventory-entry schema"
            )
        product_code = _product_code(self.product_code)
        release_date = _iso_date(self.release_date, "release_date")
        if release_date != _product_release_date(product_code):
            raise ValueError(
                "Eurostat labour cost release date differs from product code"
            )
        title = _required_text(
            self.source_title,
            "source_title",
            maximum=MAX_EUROSTAT_LABOUR_COST_TITLE_CHARS,
        )
        if not _inventory_selected(title, product_code):
            raise ValueError(
                "Eurostat labour cost inventory entry is outside selection"
            )
        source_uri = _https_uri(self.source_uri, "source_uri")
        parsed_source_uri = urlsplit(source_uri)
        if (
            parsed_source_uri.path != "/eurostat/product"
            or set(parse_qs(parsed_source_uri.query, keep_blank_values=True))
            != {"code"}
            or parsed_source_uri.fragment
        ):
            raise ValueError("Eurostat labour cost source URI route differs")
        if _product_code(source_uri) != product_code:
            raise ValueError(
                "Eurostat labour cost source URI and product code differ"
            )
        page = _bounded_int(
            self.page_number,
            "page_number",
            MAX_EUROSTAT_LABOUR_COST_INDEX_PAGES,
        )
        if page < 1:
            raise ValueError(
                "Eurostat labour cost page number must be positive"
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
            "eurostat-labour-cost-inventory-entry",
            self.identity_payload(),
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError(
                "Eurostat labour cost inventory-entry identity differs"
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
    ) -> EurostatLabourCostInventoryEntryV1:
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
class EurostatLabourCostIndexExclusionV1:
    """One explicit non-lineage result from the bounded Atom search."""

    product_code: str
    release_date: str
    source_title: str
    source_uri: str
    reason: str
    exclusion_id: str = ""
    schema_version: str = EUROSTAT_LABOUR_COST_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_LABOUR_COST_EXCLUSION_SCHEMA_VERSION:
            raise ValueError(
                "unsupported Eurostat labour cost exclusion schema"
            )
        product_code = _product_code(self.product_code)
        release_date = _iso_date(self.release_date, "release_date")
        if release_date != _product_release_date(product_code):
            raise ValueError("Eurostat labour cost exclusion date differs")
        title = _required_text(
            self.source_title,
            "source_title",
            maximum=MAX_EUROSTAT_LABOUR_COST_TITLE_CHARS,
        )
        if _inventory_selected(title, product_code):
            raise ValueError("Eurostat labour cost exclusion matches selection")
        source_uri = _https_uri(self.source_uri, "source_uri")
        if _product_code(source_uri) != product_code:
            raise ValueError("Eurostat labour cost exclusion URI differs")
        reason = _required_text(self.reason, "reason")
        if reason != _exclusion_reason(title):
            raise ValueError("Eurostat labour cost exclusion reason differs")
        object.__setattr__(self, "product_code", product_code)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "source_uri", source_uri)
        object.__setattr__(self, "reason", reason)
        expected = _stable_id(
            "eurostat-labour-cost-exclusion", self.identity_payload()
        )
        if self.exclusion_id and self.exclusion_id != expected:
            raise ValueError("Eurostat labour cost exclusion identity differs")
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
    ) -> EurostatLabourCostIndexExclusionV1:
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
class EurostatLabourCostInventoryV1:
    """Hash-bound official search inventory and deterministic selection."""

    artifacts: tuple[EurostatLabourCostArtifactV1, ...]
    entries: tuple[EurostatLabourCostInventoryEntryV1, ...]
    exclusions: tuple[EurostatLabourCostIndexExclusionV1, ...]
    query_result_count: int
    unique_uri_count: int
    title_match_count: int
    prefilter_count: int
    inventory_id: str = ""
    schema_version: str = EUROSTAT_LABOUR_COST_INVENTORY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_LABOUR_COST_INVENTORY_SCHEMA_VERSION:
            raise ValueError(
                "unsupported Eurostat labour cost inventory schema"
            )
        artifacts = tuple(self.artifacts)
        entries = tuple(self.entries)
        exclusions = tuple(self.exclusions)
        if len(artifacts) != EUROSTAT_LABOUR_COST_SEARCH_PAGE_COUNT:
            raise ValueError("Eurostat labour cost search page count differs")
        if any(
            item.role is not EurostatLabourCostArtifactRole.SEARCH_INDEX
            for item in artifacts
        ):
            raise ValueError(
                "Eurostat labour cost inventory artifact role differs"
            )
        if [item.page_number for item in artifacts] != list(
            range(1, EUROSTAT_LABOUR_COST_SEARCH_PAGE_COUNT + 1)
        ):
            raise ValueError(
                "Eurostat labour cost search pages are not contiguous"
            )
        if any(
            item.request_uri != _search_uri(cast(int, item.page_number))
            or item.resolved_uri != item.request_uri
            for item in artifacts
        ):
            raise ValueError(
                "Eurostat labour cost search artifact identity differs"
            )
        if len(entries) != EUROSTAT_LABOUR_COST_RELEASE_COUNT:
            raise ValueError(
                "Eurostat labour cost selected release count differs"
            )
        codes = [item.product_code for item in entries]
        if len(codes) != len(set(codes)):
            raise ValueError(
                "Eurostat labour cost inventory repeats a product code"
            )
        if list(entries) != sorted(
            entries, key=lambda item: (item.release_date, item.product_code)
        ):
            raise ValueError(
                "Eurostat labour cost inventory entries are not canonical"
            )
        if (
            entries[0].release_date
            != EUROSTAT_LABOUR_COST_FIRST_SEARCH_RELEASE_DATE
        ):
            raise ValueError(
                "Eurostat labour cost first searchable release differs"
            )
        if (
            entries[-1].release_date
            != EUROSTAT_LABOUR_COST_LATEST_PACKAGED_RELEASE_DATE
        ):
            raise ValueError(
                "Eurostat labour cost latest searchable release differs"
            )
        if len(exclusions) != (
            EUROSTAT_LABOUR_COST_SEARCH_UNIQUE_URI_COUNT
            - EUROSTAT_LABOUR_COST_RELEASE_COUNT
        ):
            raise ValueError("Eurostat labour cost exclusion count differs")
        exclusion_codes = [item.product_code for item in exclusions]
        if len(exclusion_codes) != len(set(exclusion_codes)):
            raise ValueError("Eurostat labour cost exclusions repeat a product")
        if list(exclusions) != sorted(
            exclusions, key=lambda item: (item.release_date, item.product_code)
        ):
            raise ValueError(
                "Eurostat labour cost exclusions are not canonical"
            )
        if set(codes) & set(exclusion_codes):
            raise ValueError(
                "Eurostat labour cost selection includes an exclusion"
            )
        expected_counts = (
            EUROSTAT_LABOUR_COST_SEARCH_RESULT_COUNT,
            EUROSTAT_LABOUR_COST_SEARCH_UNIQUE_URI_COUNT,
            EUROSTAT_LABOUR_COST_TITLE_MATCH_COUNT,
            EUROSTAT_LABOUR_COST_PREFILTER_COUNT,
        )
        observed_counts = (
            _bounded_int(
                self.query_result_count,
                "query_result_count",
                MAX_EUROSTAT_LABOUR_COST_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.unique_uri_count,
                "unique_uri_count",
                MAX_EUROSTAT_LABOUR_COST_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.title_match_count,
                "title_match_count",
                MAX_EUROSTAT_LABOUR_COST_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.prefilter_count,
                "prefilter_count",
                MAX_EUROSTAT_LABOUR_COST_SEARCH_ENTRIES,
            ),
        )
        if observed_counts != expected_counts:
            raise ValueError(
                "Eurostat labour cost search inventory counts differ"
            )
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "exclusions", exclusions)
        expected = _stable_id(
            "eurostat-labour-cost-inventory", self.identity_payload()
        )
        if self.inventory_id and self.inventory_id != expected:
            raise ValueError("Eurostat labour cost inventory identity differs")
        object.__setattr__(self, "inventory_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "artifacts": [item.to_dict() for item in self.artifacts],
            "entries": [item.to_dict() for item in self.entries],
            "exclusions": [item.to_dict() for item in self.exclusions],
            "query_result_count": self.query_result_count,
            "unique_uri_count": self.unique_uri_count,
            "title_match_count": self.title_match_count,
            "prefilter_count": self.prefilter_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "inventory_id": self.inventory_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> EurostatLabourCostInventoryV1:
        return cls(
            artifacts=tuple(
                EurostatLabourCostArtifactV1.from_dict(
                    _mapping(item, "artifact")
                )
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            entries=tuple(
                EurostatLabourCostInventoryEntryV1.from_dict(
                    _mapping(item, "inventory entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            exclusions=tuple(
                EurostatLabourCostIndexExclusionV1.from_dict(
                    _mapping(item, "exclusion")
                )
                for item in _sequence(data.get("exclusions"), "exclusions")
            ),
            query_result_count=cast(int, data.get("query_result_count")),
            unique_uri_count=cast(int, data.get("unique_uri_count")),
            title_match_count=cast(int, data.get("title_match_count")),
            prefilter_count=cast(int, data.get("prefilter_count")),
            inventory_id=str(data.get("inventory_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatLabourCostObservationV1:
    """One current-revised JSON-stat labour-cost observation."""

    economy_code: str
    reference_period: str
    measure: EurostatLabourCostMeasure
    value_lexical: str
    value: float
    status: str | None
    flat_index: int
    observation_id: str = ""
    schema_version: str = EUROSTAT_LABOUR_COST_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_LABOUR_COST_OBSERVATION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat labour cost observation schema"
            )
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _ECONOMIES:
            raise ValueError("Eurostat labour cost observation economy differs")
        reference = _quarter(self.reference_period, "reference_period")
        measure = EurostatLabourCostMeasure.from_value(self.measure)
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError(
                "Eurostat labour cost observation lexical is invalid"
            )
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError("Eurostat labour cost observation numeric differs")
        if not -100.0 <= numeric <= 100.0:
            raise ValueError(
                "Eurostat labour cost observation is outside its bound"
            )
        status = _optional_text(self.status, "status")
        if status is not None and len(status) > 16:
            raise ValueError("Eurostat labour cost status exceeds its bound")
        flat_index = _bounded_int(
            self.flat_index,
            "flat_index",
            MAX_EUROSTAT_LABOUR_COST_OBSERVATIONS * 4,
        )
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "flat_index", flat_index)
        expected = _stable_id(
            "eurostat-labour-cost-observation",
            self.identity_payload(),
        )
        if self.observation_id and self.observation_id != expected:
            raise ValueError(
                "Eurostat labour cost observation identity differs"
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
    ) -> EurostatLabourCostObservationV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatLabourCostMeasure.from_value(
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
class EurostatLabourCostDatasetV1:
    """Exact current-revised ``lc_lci_r2_q`` cross-check response."""

    label: str
    updated_at: str
    time_start: str
    time_end: str
    artifact: EurostatLabourCostArtifactV1
    observations: tuple[EurostatLabourCostObservationV1, ...]
    dataset_id: str = "lc_lci_r2_q"
    dataset_receipt_id: str = ""
    schema_version: str = EUROSTAT_LABOUR_COST_DATASET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_LABOUR_COST_DATASET_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat labour cost dataset schema")
        if self.dataset_id != "lc_lci_r2_q":
            raise ValueError("unsupported Eurostat labour cost dataset")
        label = _required_text(self.label, "label")
        updated = _iso_datetime(self.updated_at, "updated_at")
        time_start = _quarter(self.time_start, "time_start")
        time_end = _quarter(self.time_end, "time_end")
        if time_end < time_start:
            raise ValueError(
                "Eurostat labour cost dataset time range is reversed"
            )
        if (
            self.artifact.role
            is not EurostatLabourCostArtifactRole.CURRENT_DATASET
        ):
            raise ValueError(
                "Eurostat labour cost dataset artifact role differs"
            )
        if (
            self.artifact.request_uri != EUROSTAT_LABOUR_COST_DATASET_URI
            or self.artifact.resolved_uri != EUROSTAT_LABOUR_COST_DATASET_URI
        ):
            raise ValueError(
                "Eurostat labour cost dataset artifact identity differs"
            )
        observations = tuple(self.observations)
        if not 1 <= len(observations) <= MAX_EUROSTAT_LABOUR_COST_OBSERVATIONS:
            raise ValueError(
                "Eurostat labour cost observation count is invalid"
            )
        keys = [item.key for item in observations]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "Eurostat labour cost dataset repeats an observation"
            )
        periods = _quarter_range(time_start, time_end)
        expected_keys = {
            (measure.value, economy, period)
            for measure in EurostatLabourCostMeasure
            for economy in _ECONOMIES
            for period in periods
            if period
            >= {
                "EA21": "2010-Q1",
                "DE": "2000-Q1",
                "FR": "2009-Q1",
            }[economy]
        }
        missing_keys = expected_keys - set(keys)
        if missing_keys or set(keys) - expected_keys:
            raise ValueError(
                "Eurostat labour cost dataset cube coverage differs"
            )
        period_positions = {
            period: position for position, period in enumerate(periods)
        }
        measure_coordinates = {
            EurostatLabourCostMeasure.TOTAL: 0,
            EurostatLabourCostMeasure.WAGES: 1,
            EurostatLabourCostMeasure.OTHER: 2,
        }
        if any(
            item.flat_index
            != (
                measure_coordinates[item.measure]
                * len(_ECONOMIES)
                * len(periods)
                + _ECONOMY_ORDER[item.economy_code] * len(periods)
                + period_positions[item.reference_period]
            )
            for item in observations
        ):
            raise ValueError(
                "Eurostat labour cost observation coordinate differs"
            )
        if list(observations) != sorted(
            observations,
            key=lambda item: (
                list(EurostatLabourCostMeasure).index(item.measure),
                _ECONOMY_ORDER[item.economy_code],
                item.reference_period,
            ),
        ):
            raise ValueError(
                "Eurostat labour cost observations are not canonical"
            )
        if (
            min(item.reference_period for item in observations) != time_start
            or max(item.reference_period for item in observations) != time_end
        ):
            raise ValueError("Eurostat labour cost dataset time range differs")
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "updated_at", updated)
        object.__setattr__(self, "time_start", time_start)
        object.__setattr__(self, "time_end", time_end)
        object.__setattr__(self, "observations", observations)
        expected = _stable_id(
            "eurostat-labour-cost-dataset", self.identity_payload()
        )
        if self.dataset_receipt_id and self.dataset_receipt_id != expected:
            raise ValueError("Eurostat labour cost dataset identity differs")
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatLabourCostDatasetV1:
        return cls(
            label=str(data.get("label", "")),
            updated_at=str(data.get("updated_at", "")),
            time_start=str(data.get("time_start", "")),
            time_end=str(data.get("time_end", "")),
            artifact=EurostatLabourCostArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            observations=tuple(
                EurostatLabourCostObservationV1.from_dict(
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
        page_number,
        "page_number",
        MAX_EUROSTAT_LABOUR_COST_INDEX_PAGES,
    )
    if page < 1:
        raise ValueError("Eurostat labour cost search page must be positive")
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
        f"{_SEARCH_PREFIX}text": "labour costs",
        f"{_SEARCH_PREFIX}sort": "date",
        f"{_SEARCH_PREFIX}collection": "CAT_PREREL",
        f"{_SEARCH_PREFIX}priv_r_p_implicitModel": "true",
        "pageNumber": str(page),
    }
    return f"{EUROSTAT_LABOUR_COST_SEARCH_URI}?{urlencode(params)}"


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(EUROSTAT_LABOUR_COST_SOURCE_KEY)
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


def build_eurostat_labour_cost_search_requests(
    registry: OfficialSourceRegistryV1,
    *,
    page_count: int = EUROSTAT_LABOUR_COST_SEARCH_PAGE_COUNT,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the bounded official Atom inventory requests."""
    count = _bounded_int(
        page_count, "page_count", MAX_EUROSTAT_LABOUR_COST_INDEX_PAGES
    )
    if count < 1:
        raise ValueError("Eurostat labour cost search needs at least one page")
    return tuple(
        _request(
            registry,
            _search_uri(page),
            OfficialSourceFormat.ATOM,
            page_number=page,
        )
        for page in range(1, count + 1)
    )


def build_eurostat_labour_cost_dataset_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact current-revised labour cost cross-check request."""
    return _request(
        registry,
        EUROSTAT_LABOUR_COST_DATASET_URI,
        OfficialSourceFormat.JSON_STAT,
    )


def build_eurostat_labour_cost_release_requests(
    registry: OfficialSourceRegistryV1,
    inventory: EurostatLabourCostInventoryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one primary request for every selected labour cost product."""
    if not isinstance(inventory, EurostatLabourCostInventoryV1):
        raise TypeError(
            "Eurostat labour cost release requests require a v1 inventory"
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
    raise ValueError("Eurostat labour cost release document format differs")


def build_eurostat_labour_cost_document_requests(
    registry: OfficialSourceRegistryV1,
    inventory: EurostatLabourCostInventoryV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Discover one English source document for each legacy landing."""
    if not isinstance(inventory, EurostatLabourCostInventoryV1):
        raise TypeError(
            "Eurostat labour cost document requests require a v1 inventory"
        )
    entries = {item.product_code: item for item in inventory.entries}
    snapshots = tuple(release_snapshots)
    if len(snapshots) != EUROSTAT_LABOUR_COST_RELEASE_COUNT:
        raise ValueError("Eurostat labour cost landing snapshot count differs")
    by_code: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        code = _product_code(snapshot.request.uri)
        if code in by_code:
            raise ValueError(
                "Eurostat labour cost landing corpus repeats a product code"
            )
        by_code[code] = snapshot
    if set(by_code) != set(entries):
        raise ValueError("Eurostat labour cost landing corpus is incomplete")
    requests: list[OfficialSourceRequestV1] = []
    for code, entry in entries.items():
        snapshot = by_code[code]
        if snapshot.request.uri != entry.source_uri:
            raise ValueError(
                "Eurostat labour cost landing request identity differs"
            )
        _, _, _, document_uris = _parse_release_landing(snapshot, entry)
        requests.extend(
            _request(registry, uri, _document_source_format(uri))
            for uri in document_uris
        )
    if (
        EUROSTAT_LABOUR_COST_DOCUMENT_COUNT
        and len(requests) != EUROSTAT_LABOUR_COST_DOCUMENT_COUNT
    ):
        raise ValueError("Eurostat labour cost English document count differs")
    return tuple(requests)


def _artifact_from_snapshot(
    snapshot: OfficialRawSnapshotV1,
    role: EurostatLabourCostArtifactRole,
    *,
    product_code: str | None = None,
) -> EurostatLabourCostArtifactV1:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError("Eurostat labour cost artifact requires a v1 snapshot")
    if (
        snapshot.request.source_key != EUROSTAT_LABOUR_COST_SOURCE_KEY
        or snapshot.request.parser_id != EUROSTAT_LABOUR_COST_PARSER_ID
        or snapshot.request.parser_version
        != EUROSTAT_LABOUR_COST_PARSER_VERSION
    ):
        raise ValueError(
            "Eurostat labour cost snapshot source or parser differs"
        )
    if snapshot.request.method is not OfficialRequestMethod.GET:
        raise ValueError("Eurostat labour cost snapshot method differs")
    expected_format = {
        EurostatLabourCostArtifactRole.SEARCH_INDEX: OfficialSourceFormat.ATOM,
        EurostatLabourCostArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
        EurostatLabourCostArtifactRole.RELEASE_HTML: OfficialSourceFormat.HTML,
        EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
        EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
    }[role]
    if snapshot.request.source_format is not expected_format:
        raise ValueError(
            "Eurostat labour cost snapshot format differs from artifact role"
        )
    if snapshot.status_code != 200:
        raise ValueError("Eurostat labour cost snapshot status differs")
    if role is EurostatLabourCostArtifactRole.SEARCH_INDEX:
        if (
            snapshot.request.page_number is None
            or snapshot.request.uri != _search_uri(snapshot.request.page_number)
        ):
            raise ValueError(
                "Eurostat labour cost search request identity differs"
            )
        try:
            root = safe_xml_fromstring(snapshot.content)
        except (DefusedXmlException, ParseError) as exc:
            raise ValueError(
                "Eurostat labour cost search artifact is invalid XML"
            ) from exc
        if root.tag != f"{{{_ATOM_NAMESPACE}}}feed":
            raise ValueError("Eurostat labour cost search artifact is not Atom")
    elif role is EurostatLabourCostArtifactRole.CURRENT_DATASET:
        if (
            snapshot.request.page_number is not None
            or snapshot.request.uri != EUROSTAT_LABOUR_COST_DATASET_URI
        ):
            raise ValueError(
                "Eurostat labour cost dataset request identity differs"
            )
        if not snapshot.content.lstrip().startswith(b"{"):
            raise ValueError(
                "Eurostat labour cost dataset artifact is not JSON"
            )
    elif snapshot.request.page_number is not None:
        raise ValueError(
            "Eurostat labour cost release request has a page number"
        )
    elif role in {
        EurostatLabourCostArtifactRole.RELEASE_HTML,
        EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_HTML,
    }:
        if b"<html" not in snapshot.content[:16_384].lower():
            raise ValueError("Eurostat labour cost HTML signature differs")
    elif not snapshot.content.startswith(b"%PDF-"):
        raise ValueError("Eurostat labour cost PDF signature differs")
    if role in {
        EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_HTML,
        EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_PDF,
    } and (
        product_code is None
        or _product_code(snapshot.request.uri) != _product_code(product_code)
    ):
        raise ValueError("Eurostat labour cost document product code differs")
    return EurostatLabourCostArtifactV1(
        role=role,
        request_uri=snapshot.request.uri,
        resolved_uri=snapshot.resolved_uri,
        source_format=expected_format,
        content_length=len(snapshot.content),
        content_sha256=snapshot.content_sha256,
        page_number=(
            snapshot.request.page_number
            if role is EurostatLabourCostArtifactRole.SEARCH_INDEX
            else None
        ),
        product_code=product_code,
    )


def _atom_text(entry: Element, tag: str) -> str:
    node = entry.find(f"{{{_ATOM_NAMESPACE}}}{tag}")
    if node is None or node.text is None:
        raise ValueError(f"Eurostat labour cost Atom entry omits {tag}")
    return html.unescape(node.text)


def _atom_link(entry: Element) -> str:
    links = tuple(
        item
        for item in entry.findall(f"{{{_ATOM_NAMESPACE}}}link")
        if item.attrib.get("rel") == "alternate"
    )
    if len(links) != 1 or "href" not in links[0].attrib:
        raise ValueError(
            "Eurostat labour cost Atom entry alternate link differs"
        )
    return _https_uri(links[0].attrib["href"], "source_uri")


def parse_eurostat_labour_cost_inventory(
    snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatLabourCostInventoryV1:
    """Parse and prove the complete bounded labour cost search selection."""
    as_of = _iso_date(as_of_date, "as_of_date")
    values = tuple(snapshots)
    if len(values) != EUROSTAT_LABOUR_COST_SEARCH_PAGE_COUNT:
        raise ValueError(
            "Eurostat labour cost inventory snapshot count differs"
        )
    if [item.request.page_number for item in values] != list(
        range(1, EUROSTAT_LABOUR_COST_SEARCH_PAGE_COUNT + 1)
    ):
        raise ValueError(
            "Eurostat labour cost search snapshots are not contiguous"
        )
    artifacts: list[EurostatLabourCostArtifactV1] = []
    raw_entries: list[tuple[int, str, str, str, str, str]] = []
    for snapshot in values:
        artifact = _artifact_from_snapshot(
            snapshot, EurostatLabourCostArtifactRole.SEARCH_INDEX
        )
        artifacts.append(artifact)
        try:
            root = safe_xml_fromstring(snapshot.content)
        except (DefusedXmlException, ParseError) as exc:
            raise ValueError(
                "Eurostat labour cost search page is invalid XML"
            ) from exc
        entries = tuple(root.findall(f"{{{_ATOM_NAMESPACE}}}entry"))
        expected_count = (
            59
            if artifact.page_number == EUROSTAT_LABOUR_COST_SEARCH_PAGE_COUNT
            else 100
        )
        if len(entries) != expected_count:
            raise ValueError("Eurostat labour cost search page size differs")
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
    if len(raw_entries) != EUROSTAT_LABOUR_COST_SEARCH_RESULT_COUNT:
        raise ValueError("Eurostat labour cost search result count differs")
    unique_by_uri: dict[str, tuple[int, str, str, str, str, str]] = {}
    for item in raw_entries:
        previous = unique_by_uri.get(item[2])
        if previous is not None and previous[1:] != item[1:]:
            raise ValueError(
                "Eurostat labour cost search duplicate URI differs"
            )
        unique_by_uri.setdefault(item[2], item)
    if len(unique_by_uri) != EUROSTAT_LABOUR_COST_SEARCH_UNIQUE_URI_COUNT:
        raise ValueError("Eurostat labour cost search unique URI count differs")
    unique_entries = tuple(unique_by_uri.values())
    title_match_count = sum(
        re.search(r"\blabour costs?\b", title, re.IGNORECASE) is not None
        for _, title, _, _, _, _ in unique_entries
    )
    prefiltered = [
        item for item in unique_entries if _inventory_prefilter(item[1])
    ]
    selected: list[EurostatLabourCostInventoryEntryV1] = []
    exclusions: list[EurostatLabourCostIndexExclusionV1] = []
    for page, title, uri, published, updated, summary in unique_entries:
        product_code = _product_code(uri)
        release_date = _product_release_date(product_code)
        if release_date > as_of:
            continue
        if not _inventory_selected(title, product_code):
            exclusions.append(
                EurostatLabourCostIndexExclusionV1(
                    product_code=product_code,
                    release_date=release_date,
                    source_title=title,
                    source_uri=uri,
                    reason=_exclusion_reason(title),
                )
            )
            continue
        selected.append(
            EurostatLabourCostInventoryEntryV1(
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
    return EurostatLabourCostInventoryV1(
        artifacts=tuple(artifacts),
        entries=tuple(selected),
        exclusions=tuple(exclusions),
        query_result_count=len(raw_entries),
        unique_uri_count=len(unique_entries),
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


def parse_eurostat_labour_cost_dataset(
    snapshot: OfficialRawSnapshotV1,
) -> EurostatLabourCostDatasetV1:
    """Decode the exact current-revised labour cost JSON-stat cross-check."""
    artifact = _artifact_from_snapshot(
        snapshot, EurostatLabourCostArtifactRole.CURRENT_DATASET
    )
    try:
        payload = _mapping(json.loads(snapshot.content), "dataset")
        lexical_payload = _mapping(
            json.loads(snapshot.content, parse_float=str), "lexical dataset"
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "Eurostat labour cost dataset is invalid JSON"
        ) from exc
    identifiers = [str(item) for item in _sequence(payload.get("id"), "id")]
    expected_ids = [
        "freq",
        "s_adj",
        "unit",
        "nace_r2",
        "lcstruct",
        "geo",
        "time",
    ]
    if identifiers != expected_ids:
        raise ValueError("Eurostat labour cost dataset dimensions differ")
    sizes = tuple(_sequence(payload.get("size"), "size"))
    if any(
        isinstance(item, bool) or not isinstance(item, int) for item in sizes
    ):
        raise TypeError("Eurostat labour cost dimension sizes must be integers")
    dimension = _mapping(payload.get("dimension"), "dimension")
    expected_codes = {
        "freq": ("Q",),
        "s_adj": ("CA",),
        "unit": ("PCH_SM",),
        "nace_r2": ("B-S",),
        "lcstruct": ("D1_D4_MD5", "D11", "D12_D4_MD5"),
        "geo": _ECONOMIES,
    }
    observed_codes = {
        name: _dimension_codes(_mapping(dimension[name], name), name)
        for name in expected_codes
    }
    if observed_codes != expected_codes:
        raise ValueError(
            "Eurostat labour cost dataset dimensions or codes differ"
        )
    time_codes = _dimension_codes(_mapping(dimension["time"], "time"), "time")
    if not time_codes or any(
        _QUARTER_RE.fullmatch(item) is None for item in time_codes
    ):
        raise ValueError("Eurostat labour cost dataset time codes differ")
    if time_codes != _quarter_range(time_codes[0], time_codes[-1]):
        raise ValueError(
            "Eurostat labour cost dataset time codes are not contiguous"
        )
    expected_sizes = (1, 1, 1, 1, 3, len(_ECONOMIES), len(time_codes))
    if tuple(cast(Sequence[int], sizes)) != expected_sizes:
        raise ValueError("Eurostat labour cost dataset sizes differ")
    values = _mapping(payload.get("value"), "value")
    lexical_values = _mapping(lexical_payload.get("value"), "lexical value")
    statuses = _mapping(payload.get("status", {}), "status")
    if set(values) != set(lexical_values):
        raise ValueError(
            "Eurostat labour cost lexical and numeric values differ"
        )
    if not set(statuses) <= set(values):
        raise ValueError("Eurostat labour cost status has no observation")
    observations: list[EurostatLabourCostObservationV1] = []
    time_count = len(time_codes)
    geo_count = len(_ECONOMIES)
    maximum = 3 * geo_count * time_count
    for raw_index, raw_value in values.items():
        try:
            flat_index = int(str(raw_index))
        except ValueError as exc:
            raise ValueError(
                "Eurostat labour cost sparse index is invalid"
            ) from exc
        if not 0 <= flat_index < maximum:
            raise ValueError(
                "Eurostat labour cost sparse index is outside its cube"
            )
        component_index, remainder = divmod(flat_index, geo_count * time_count)
        geo_index, time_index = divmod(remainder, time_count)
        measure = (
            EurostatLabourCostMeasure.TOTAL,
            EurostatLabourCostMeasure.WAGES,
            EurostatLabourCostMeasure.OTHER,
        )[component_index]
        lexical = str(lexical_values[raw_index])
        observations.append(
            EurostatLabourCostObservationV1(
                economy_code=_ECONOMIES[geo_index],
                reference_period=time_codes[time_index],
                measure=measure,
                value_lexical=lexical,
                value=float(cast(float, raw_value)),
                status=cast(str | None, statuses.get(raw_index)),
                flat_index=flat_index,
            )
        )
    observations.sort(
        key=lambda item: (
            list(EurostatLabourCostMeasure).index(item.measure),
            _ECONOMY_ORDER[item.economy_code],
            item.reference_period,
        )
    )
    return EurostatLabourCostDatasetV1(
        label=str(payload.get("label", "")),
        updated_at=str(payload.get("updated", "")),
        time_start=time_codes[0],
        time_end=time_codes[-1],
        artifact=artifact,
        observations=tuple(observations),
    )


class EurostatLabourCostMovement(str, Enum):
    """Direction wording attached to the source-authored headline rate."""

    REPORTED = "reported"
    UP = "up"
    DOWN = "down"
    STABLE = "stable"

    @classmethod
    def from_value(
        cls, value: str | EurostatLabourCostMovement
    ) -> EurostatLabourCostMovement:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat labour cost movement"
            ) from exc


@dataclass(frozen=True, slots=True)
class EurostatLabourCostReleaseValueV1:
    """Source-era euro-area labour cost rate in a release headline."""

    economy_code: str
    reference_period: str
    measure: EurostatLabourCostMeasure
    movement: EurostatLabourCostMovement
    value_lexical: str
    value: float
    evidence_id: str
    release_value_id: str = ""
    schema_version: str = EUROSTAT_LABOUR_COST_RELEASE_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_LABOUR_COST_RELEASE_VALUE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat labour cost release-value schema"
            )
        if self.economy_code != "EA":
            raise ValueError(
                "Eurostat labour cost release value must retain source-era EA scope"
            )
        reference = _quarter(self.reference_period, "reference_period")
        measure = EurostatLabourCostMeasure.from_value(self.measure)
        if measure is not EurostatLabourCostMeasure.TOTAL:
            raise ValueError("Eurostat labour cost headline measure differs")
        movement = EurostatLabourCostMovement.from_value(self.movement)
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None or lexical.startswith("-"):
            raise ValueError("Eurostat labour cost headline rate is invalid")
        magnitude = float(lexical)
        numeric = {
            EurostatLabourCostMovement.DOWN: -magnitude,
            EurostatLabourCostMovement.STABLE: 0.0,
        }.get(movement, magnitude)
        if self.value != numeric or not math.isfinite(self.value):
            raise ValueError(
                "Eurostat labour cost headline numeric value differs"
            )
        if not -100.0 <= self.value <= 100.0:
            raise ValueError(
                "Eurostat labour cost headline value is outside its bound"
            )
        evidence_id = _required_text(self.evidence_id, "evidence_id")
        if not evidence_id.startswith(
            "eurostat-labour-cost-inventory-entry:sha256:"
        ):
            raise ValueError("Eurostat labour cost headline evidence differs")
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "movement", movement)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "evidence_id", evidence_id)
        expected = _stable_id(
            "eurostat-labour-cost-release-value",
            self.identity_payload(),
        )
        if self.release_value_id and self.release_value_id != expected:
            raise ValueError(
                "Eurostat labour cost release-value identity differs"
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
    ) -> EurostatLabourCostReleaseValueV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatLabourCostMeasure.from_value(
                str(data.get("measure", ""))
            ),
            movement=EurostatLabourCostMovement.from_value(
                str(data.get("movement", ""))
            ),
            value_lexical=str(data.get("value_lexical", "")),
            value=float(cast(float, data.get("value"))),
            evidence_id=str(data.get("evidence_id", "")),
            release_value_id=str(data.get("release_value_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatLabourCostPublishedValueV1:
    """One source-era value read from a release publication table."""

    economy_code: str
    reference_period: str
    measure: EurostatLabourCostMeasure
    value_lexical: str
    value: float
    evidence_artifact_sha256: str
    evidence_locator: str
    published_value_id: str = ""
    schema_version: str = EUROSTAT_LABOUR_COST_PUBLISHED_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_LABOUR_COST_PUBLISHED_VALUE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat labour cost published-value schema"
            )
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _RELEASE_ECONOMIES:
            raise ValueError(
                "Eurostat labour cost published-value economy differs"
            )
        reference = _quarter(self.reference_period, "reference_period")
        measure = EurostatLabourCostMeasure.from_value(self.measure)
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _SIGNED_NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError(
                "Eurostat labour cost published-value lexical is invalid"
            )
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError(
                "Eurostat labour cost published-value numeric differs"
            )
        if not -100.0 <= numeric <= 100.0:
            raise ValueError(
                "Eurostat labour cost published value is outside its bound"
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
            "eurostat-labour-cost-published-value",
            self.identity_payload(),
        )
        if self.published_value_id and self.published_value_id != expected:
            raise ValueError(
                "Eurostat labour cost published-value identity differs"
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
    ) -> EurostatLabourCostPublishedValueV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatLabourCostMeasure.from_value(
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


def _quarter_end(value: str) -> date:
    token = _quarter(value, "reference_period")
    year = int(token[:4])
    quarter = int(token[-1])
    month = quarter * 3
    following = date(year + (month == 12), month % 12 + 1, 1)
    return date.fromordinal(following.toordinal() - 1)


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
        ("2025-Q4", "EA20"),
        ("9999-Q4", "EA21"),
    )
    return next(
        label for boundary, label in boundaries if reference <= boundary
    )


def _activity_scope_for_period(
    reference_period: str,
) -> EurostatLabourCostActivityScope:
    """Return the source-era aggregate around Eurostat's 2012 scope break."""
    period = _quarter(reference_period, "reference_period")
    if period < EUROSTAT_LABOUR_COST_ACTIVITY_SCOPE_BREAK_REFERENCE_PERIOD:
        return EurostatLabourCostActivityScope.BUSINESS_ECONOMY
    return EurostatLabourCostActivityScope.WHOLE_ECONOMY


def _release_lag(release_date: str, reference_period: str) -> int:
    released = date.fromisoformat(_iso_date(release_date, "release_date"))
    reference = _quarter(reference_period, "reference_period")
    lag = (released - _quarter_end(reference)).days
    if not 70 <= lag <= 105:
        raise ValueError(
            "Eurostat labour cost release lag is outside the qualified window"
        )
    return lag


_HEADLINE_VALUE_RE = re.compile(
    r"(?P<value>\d+(?:\.\d+)?)\s*%",
    re.IGNORECASE,
)


def _headline_value(
    entry: EurostatLabourCostInventoryEntryV1,
    reference_period: str,
) -> EurostatLabourCostReleaseValueV1:
    match = _HEADLINE_VALUE_RE.search(entry.source_title)
    if match is None:
        raise ValueError("Eurostat labour cost release headline has no rate")
    movement = (
        EurostatLabourCostMovement.DOWN
        if entry.source_title.casefold().startswith(
            "annual decrease in labour costs"
        )
        else EurostatLabourCostMovement.REPORTED
    )
    lexical = match.group("value")
    magnitude = float(lexical)
    numeric = {
        EurostatLabourCostMovement.DOWN: -magnitude,
        EurostatLabourCostMovement.STABLE: 0.0,
    }.get(movement, magnitude)
    return EurostatLabourCostReleaseValueV1(
        economy_code="EA",
        reference_period=reference_period,
        measure=EurostatLabourCostMeasure.TOTAL,
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
    entry: EurostatLabourCostInventoryEntryV1,
) -> tuple[str, str, str | None, tuple[str, ...]]:
    try:
        content = snapshot.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "Eurostat labour cost release landing is not UTF-8"
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
        raise ValueError("Eurostat labour cost Atom and landing titles differ")
    text = " ".join(parser.text)
    page_dates: set[str] = set()
    for date_match in _LANDING_DATE_RE.finditer(text):
        month = _MONTHS.get(date_match.group("month").casefold())
        if month is None:
            raise ValueError(
                "Eurostat labour cost landing month is unsupported"
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
                "Eurostat labour cost landing date is invalid"
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
            "Eurostat labour cost release landing has no unique publication time"
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
            "Eurostat labour cost release landing has no unique release date"
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
            "Eurostat labour cost landing exposes multiple English release documents"
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
                    "Eurostat labour cost release PDF has too many unreadable pages"
                )
            text = " ".join(page_text)
        except (PdfReadError, ValueError) as exc:
            raise ValueError(
                "Eurostat labour cost release PDF text extraction failed"
            ) from exc
    else:
        raise ValueError(
            "Eurostat labour cost release value source format differs"
        )
    normalized = _SPACE_RE.sub(" ", text.replace("\u2212", "-")).strip()
    if not normalized:
        raise ValueError(
            "Eurostat labour cost release value source has no text"
        )
    return normalized


_REFERENCE_QUARTER_RE = re.compile(
    r"\b(?P<quarter>first|second|third|fourth|1st|2nd|3rd|4th)\s+"
    r"quarter(?:\s+of|\s+in)?\s+(?P<year>20\d{2})\b",
    re.IGNORECASE,
)
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


def _reference_period_from_publication(
    entry: EurostatLabourCostInventoryEntryV1,
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
        for match in _REFERENCE_QUARTER_RE.finditer(text):
            quarter = {
                "first": 1,
                "1st": 1,
                "second": 2,
                "2nd": 2,
                "third": 3,
                "3rd": 3,
                "fourth": 4,
                "4th": 4,
            }[match.group("quarter").casefold()]
            reference_period = f"{match.group('year')}-Q{quarter}"
            try:
                lag = _release_lag(entry.release_date, reference_period)
            except ValueError:
                continue
            return reference_period, lag
    inferred = EUROSTAT_LABOUR_COST_REFERENCE_PERIOD_INFERENCES.get(
        entry.product_code
    )
    if inferred is not None:
        return inferred, _release_lag(entry.release_date, inferred)
    raise ValueError(
        "Eurostat labour cost publication omits a qualified reference quarter for "
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
            "Eurostat labour cost source-stated EA composition differs: "
            f"expected={expected}, observed={tuple(sorted(labels))}"
        )
    return expected, expected in labels


class _LabourCostTableParser(HTMLParser):
    """Retain source-authored HTML table cells without layout styling."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[tuple[tuple[str, ...], ...]] = []
        self.table_contexts: list[str] = []
        self._table: list[tuple[str, ...]] | None = None
        self._table_context = ""
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._cell_span = 1
        self._outside_text: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        name = tag.casefold()
        if name in {"script", "style", "template", "noscript"}:
            self._ignored_depth += 1
            return
        if name == "table" and self._table is None:
            self._table = []
            self._table_context = " ".join(self._outside_text[-64:])
        elif name == "tr" and self._table is not None:
            self._row = []
        elif name in {"td", "th"} and self._row is not None:
            self._cell = []
            values = {key.casefold(): value for key, value in attrs}
            try:
                self._cell_span = int(values.get("colspan") or "1")
            except ValueError:
                self._cell_span = 1
            if not 1 <= self._cell_span <= 64:
                self._cell_span = 1

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)
        elif self._table is None and not self._ignored_depth:
            value = _SPACE_RE.sub(" ", data).strip()
            if value:
                self._outside_text.append(value)

    def handle_endtag(self, tag: str) -> None:
        name = tag.casefold()
        if name in {"script", "style", "template", "noscript"}:
            if self._ignored_depth:
                self._ignored_depth -= 1
            return
        if name in {"td", "th"} and self._cell is not None:
            assert self._row is not None
            value = _SPACE_RE.sub(" ", " ".join(self._cell)).strip()
            self._row.extend((value,) * self._cell_span)
            self._cell = None
            self._cell_span = 1
        elif name == "tr" and self._row is not None:
            if any(self._row):
                assert self._table is not None
                self._table.append(tuple(self._row))
            self._row = None
        elif name == "table" and self._table is not None:
            if self._table:
                self.tables.append(tuple(self._table))
                self.table_contexts.append(self._table_context)
            self._table = None
            self._table_context = ""


_LABOUR_COST_PERIOD_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"Q(?P<quarter>[1-4])\s*[-/]?\s*(?P<year>\d{2}|20\d{2})|"
    r"(?P<year_first>20\d{2})\s*[-/]?\s*Q(?P<quarter_last>[1-4])|"
    r"(?P<short_year>\d{2})\s*/\s*Q(?P<short_quarter>[1-4])|"
    r"(?P<word>first|second|third|fourth|1st|2nd|3rd|4th)\s+"
    r"quarter(?:\s+of|\s+in)?\s+(?P<word_year>20\d{2}))"
    r"(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_LABOUR_COST_VALUE_CELL_RE = re.compile(
    r"^[+\-\u2212]?(?:\d+(?:[.,]\d+)?|[.,]\d+)"
)
_LABOUR_COST_PDF_ROW_RE = re.compile(
    rf"^\s*(?P<label>EA{_EA_COMPOSITION_NUMBER_PATTERN}|"
    rf"Euro[ -](?:area|zone)(?:\s*\(?(?:EA)?"
    rf"{_EA_COMPOSITION_NUMBER_PATTERN}\)?)?|"
    r"Germany|DE|France|FR)\b(?P<body>.*)$",
    re.IGNORECASE,
)
_LABOUR_COST_PDF_NUMBER_RE = re.compile(
    r"(?<![A-Za-z0-9.])[+\-−]?(?:\d+(?:\.\d+)?|\.\d+)(?![A-Za-z0-9.])|:"
)


def _labour_cost_period_cell(value: str) -> str | None:
    token = _SPACE_RE.sub(" ", value).strip(" *\u00a0")
    match = _LABOUR_COST_PERIOD_RE.fullmatch(token)
    if match is None:
        return None
    raw_quarter = (
        match.group("quarter")
        or match.group("quarter_last")
        or match.group("short_quarter")
    )
    raw_year = (
        match.group("year")
        or match.group("year_first")
        or match.group("short_year")
    )
    if raw_quarter is None:
        raw_quarter = {
            "first": "1",
            "1st": "1",
            "second": "2",
            "2nd": "2",
            "third": "3",
            "3rd": "3",
            "fourth": "4",
            "4th": "4",
        }[cast(str, match.group("word")).casefold()]
        raw_year = cast(str, match.group("word_year"))
    year = int(cast(str, raw_year))
    if len(cast(str, raw_year)) == 2:
        year += 1900 if year >= 90 else 2000
    return f"{year:04d}-Q{raw_quarter}"


def _labour_cost_periods(value: str) -> tuple[str, ...]:
    periods: list[str] = []
    for match in _LABOUR_COST_PERIOD_RE.finditer(value):
        period = _labour_cost_period_cell(match.group(0))
        if period is not None:
            periods.append(period)
    return tuple(periods)


def _labour_cost_measure_cell(
    value: str,
) -> EurostatLabourCostMeasure | None:
    token = re.sub(r"[^a-z]+", " ", value.casefold()).strip()
    if not token:
        return None
    if token in {"tot", "total", "total labour costs", "labour costs total"}:
        return EurostatLabourCostMeasure.TOTAL
    if "wage" in token or "salar" in token or token == "wag":
        return EurostatLabourCostMeasure.WAGES
    if "other" in token or "non wage" in token or token == "oth":
        return EurostatLabourCostMeasure.OTHER
    return None


def _labour_cost_economy_cell(
    value: str, *, expected_ea_composition: str
) -> str | None:
    token = re.sub(r"[²³*]+", "", _SPACE_RE.sub(" ", value)).strip().casefold()
    if re.fullmatch(
        rf"(?:euro[ -](?:area|zone)(?:\s*\(?(?:ea)?"
        rf"{_EA_COMPOSITION_NUMBER_PATTERN}\)?)?|"
        rf"ea{_EA_COMPOSITION_NUMBER_PATTERN})",
        token,
    ):
        composition_match = re.search(r"(?:ea\s*)?(?P<number>\d{2})", token)
        if (
            composition_match is not None
            and f"EA{composition_match.group('number')}"
            != expected_ea_composition
        ):
            return None
        return "EA"
    if token in {"germany", "de"}:
        return "DE"
    if token in {"france", "fr"}:
        return "FR"
    return None


def _labour_cost_value_cell(value: str) -> str | None:
    token = _SPACE_RE.sub(" ", value).strip()
    if token in {"", ":", "-", ".."}:
        return None
    match = _LABOUR_COST_VALUE_CELL_RE.match(token)
    if match is None:
        return None
    return match.group(0).replace("\u2212", "-").replace(",", ".")


_LABOUR_COST_TABLE_TOKEN: Final = (
    r"(?:[+\-\u2212]?(?:\d+(?:[.,]\d+)?|[.,]\d+)\*?|:)"
)
_LABOUR_COST_TOTAL_TABLE_START_RE = re.compile(
    r"\b(?:Table\s*1[.:]?\s+)?Total nominal hourly labour costs?,?\s+"
    r"(?:the\s+)?whole economy\b",
    re.IGNORECASE,
)
_LABOUR_COST_TOTAL_TABLE_END_RE = re.compile(
    r"\b(?:Table\s*2[.:]?\s+)?Total nominal hourly labour costs?,?\s+"
    r"industry\b",
    re.IGNORECASE,
)
_LABOUR_COST_COMPONENT_TABLE_RE = re.compile(
    r"\bTable\s*3[.:]?\s+Breakdown of total nominal hourly labour costs,?\s+"
    r"(?:the\s+)?whole economy\b(?P<section>.*?)(?:\bTable\s*4[.:]?|"
    r"\bFurther information\b|$)",
    re.IGNORECASE,
)
_LABOUR_COST_TEXT_ROW_LABELS: Final[Mapping[str, tuple[str, ...]]] = {
    "EA": (
        *(
            label
            for number in (
                "12",
                "13",
                "15",
                "16",
                "17",
                "18",
                "19",
                "20",
                "21",
            )
            for label in (
                f"Euro area (EA{number})",
                f"Euro-zone (EA{number})",
                f"Euro area {number}",
                f"Euro-zone {number}",
                f"EA{number}",
            )
        ),
        "Euro area",
        "Euro-zone",
    ),
    "DE": ("Germany", "DE", "D"),
    "FR": ("France", "FR", "F"),
}


def _labour_cost_text_row(
    section: str,
    labels: Sequence[str],
    width: int,
) -> tuple[str, ...] | None:
    alternatives = "|".join(
        re.escape(label) for label in sorted(labels, key=len, reverse=True)
    )
    match = re.search(
        rf"(?<![\w-])(?:{alternatives})(?![\w-])\s+"
        rf"(?P<values>{_LABOUR_COST_TABLE_TOKEN}(?:\s+"
        rf"{_LABOUR_COST_TABLE_TOKEN}){{{width - 1}}})",
        section,
        re.IGNORECASE,
    )
    if match is None:
        return None
    return tuple(match.group("values").split())


def _labour_cost_component_rows(
    section: str,
    labels: Sequence[str],
    width: int,
) -> Mapping[EurostatLabourCostMeasure, tuple[str, ...]]:
    alternatives = "|".join(
        re.escape(label) for label in sorted(labels, key=len, reverse=True)
    )
    values_pattern = (
        rf"{_LABOUR_COST_TABLE_TOKEN}(?:\s+"
        rf"{_LABOUR_COST_TABLE_TOKEN}){{{width - 1}}}"
    )
    match = re.search(
        rf"(?<![\w-])(?:{alternatives})(?![\w-])\s+"
        rf"(?:Wages(?:\s+and\s+salaries)?)\s+(?P<wages>{values_pattern})\s+"
        rf"(?:Other(?:\s+labour)?\s+costs?|Other)\s+"
        rf"(?P<other>{values_pattern})\s+Total\s+"
        rf"(?P<total>{values_pattern})",
        section,
        re.IGNORECASE,
    )
    if match is None:
        return {}
    return {
        EurostatLabourCostMeasure.WAGES: tuple(match.group("wages").split()),
        EurostatLabourCostMeasure.OTHER: tuple(match.group("other").split()),
        EurostatLabourCostMeasure.TOTAL: tuple(match.group("total").split()),
    }


def _labour_cost_table_header_periods(section: str) -> tuple[str, ...]:
    first_row = re.search(
        rf"\b(?:Euro[ -](?:area|zone)(?:\s*\(?(?:EA)?"
        rf"{_EA_COMPOSITION_NUMBER_PATTERN}\)?)?|"
        rf"EA{_EA_COMPOSITION_NUMBER_PATTERN})\b\s+"
        rf"(?:Wages\b|{_LABOUR_COST_TABLE_TOKEN})",
        section,
        re.IGNORECASE,
    )
    header = section[: first_row.start()] if first_row is not None else section
    return tuple(dict.fromkeys(_labour_cost_periods(header)))


def _labour_cost_source_table_values(
    snapshot: OfficialRawSnapshotV1,
    artifact: EurostatLabourCostArtifactV1,
    reference_period: str,
) -> tuple[EurostatLabourCostPublishedValueV1, ...]:
    """Decode the legacy publication's text-flow whole-economy tables."""
    text = _release_source_text(snapshot)
    minimum_period = _quarter_shift(reference_period, -7)
    values: dict[
        tuple[str, str, EurostatLabourCostMeasure],
        EurostatLabourCostPublishedValueV1,
    ] = {}

    def retain(
        *,
        economy: str,
        periods: Sequence[str],
        measure: EurostatLabourCostMeasure,
        lexicals: Sequence[str],
        table_number: int,
    ) -> None:
        for period, raw_lexical in zip(periods, lexicals, strict=True):
            if (
                raw_lexical == ":"
                or not minimum_period <= period <= reference_period
            ):
                continue
            lexical = (
                raw_lexical.rstrip("*").replace("\u2212", "-").replace(",", ".")
            )
            item = EurostatLabourCostPublishedValueV1(
                economy_code=economy,
                reference_period=period,
                measure=measure,
                value_lexical=lexical,
                value=float(lexical),
                evidence_artifact_sha256=artifact.content_sha256,
                evidence_locator=(
                    f"labour-cost publication Table {table_number}; "
                    f"{economy} row; {period}; {measure.value}"
                ),
            )
            key = (economy, period, measure)
            previous = values.get(key)
            if previous is not None and previous.value != item.value:
                raise ValueError(
                    "Eurostat labour cost publication tables conflict for "
                    f"{artifact.product_code}: {economy} {period} {measure.value}"
                )
            values.setdefault(key, item)

    for total_match in _LABOUR_COST_TOTAL_TABLE_START_RE.finditer(text):
        end_match = _LABOUR_COST_TOTAL_TABLE_END_RE.search(
            text, total_match.end()
        )
        section = text[
            total_match.end() : end_match.start() if end_match else len(text)
        ]
        periods = _labour_cost_table_header_periods(section)
        if reference_period in periods and 1 <= len(periods) <= 20:
            retained_count = len(values)
            for economy, labels in _LABOUR_COST_TEXT_ROW_LABELS.items():
                lexicals = _labour_cost_text_row(section, labels, len(periods))
                if lexicals is not None:
                    retain(
                        economy=economy,
                        periods=periods,
                        measure=EurostatLabourCostMeasure.TOTAL,
                        lexicals=lexicals,
                        table_number=1,
                    )
            if len(values) > retained_count:
                break

    component_match = _LABOUR_COST_COMPONENT_TABLE_RE.search(text)
    if component_match is not None:
        section = component_match.group("section")
        periods = _labour_cost_table_header_periods(section)
        if reference_period in periods and 1 <= len(periods) <= 20:
            for economy, labels in _LABOUR_COST_TEXT_ROW_LABELS.items():
                for measure, lexicals in _labour_cost_component_rows(
                    section, labels, len(periods)
                ).items():
                    retain(
                        economy=economy,
                        periods=periods,
                        measure=measure,
                        lexicals=lexicals,
                        table_number=3,
                    )

    return tuple(
        sorted(
            values.values(),
            key=lambda item: (
                item.reference_period,
                _RELEASE_ECONOMY_ORDER[item.economy_code],
                list(EurostatLabourCostMeasure).index(item.measure),
            ),
        )
    )


def _labour_cost_html_table_values(
    snapshot: OfficialRawSnapshotV1,
    artifact: EurostatLabourCostArtifactV1,
    reference_period: str,
) -> tuple[EurostatLabourCostPublishedValueV1, ...]:
    if snapshot.request.source_format is not OfficialSourceFormat.HTML:
        return ()
    try:
        source = snapshot.content.decode("utf-8")
    except UnicodeDecodeError:
        source = snapshot.content.decode("cp1252")
    parser = _LabourCostTableParser()
    parser.feed(source)
    parser.close()
    expected_ea_composition = _ea_composition_for_period(reference_period)
    values: dict[
        tuple[str, str, EurostatLabourCostMeasure],
        EurostatLabourCostPublishedValueV1,
    ] = {}
    for table_number, (context, rows) in enumerate(
        zip(parser.table_contexts, parser.tables, strict=True), start=1
    ):
        own_header = " ".join(
            cell for row in rows[:4] for cell in row if cell
        ).casefold()
        table_header = (
            own_header
            if "labour cost" in own_header
            else f"{context} {own_header}".casefold()
        )
        if not (
            "labour cost" in table_header
            and "whole economy" in table_header
            and (
                "same quarter" in table_header
                or "annual" in table_header
                or "year-on-year" in table_header
            )
        ):
            continue
        period_header_index = -1
        period_columns: dict[int, str] = {}
        for row_index, row in enumerate(rows):
            period_candidates = {
                column: period
                for column, cell in enumerate(row)
                if (period := _labour_cost_period_cell(cell)) is not None
            }
            if len(period_candidates) > len(period_columns):
                period_header_index = row_index
                period_columns = period_candidates
        if reference_period not in period_columns.values():
            continue
        measure_header_index = -1
        measure_columns: dict[int, EurostatLabourCostMeasure] = {}
        for row_index, row in enumerate(rows[:8]):
            measure_candidates = {
                column: measure
                for column, cell in enumerate(row)
                if (measure := _labour_cost_measure_cell(cell)) is not None
            }
            if set(EurostatLabourCostMeasure).issubset(
                set(measure_candidates.values())
            ) and len(measure_candidates) > len(measure_columns):
                measure_header_index = row_index
                measure_columns = measure_candidates
        if not set(EurostatLabourCostMeasure).issubset(
            set(measure_columns.values())
        ):
            continue
        columns = {
            column: (period, measure_columns[column])
            for column, period in period_columns.items()
            if column in measure_columns
        }
        if not columns:
            continue
        minimum_period = _quarter_shift(reference_period, -12)
        for row in rows[max(period_header_index, measure_header_index) + 1 :]:
            economy = next(
                (
                    candidate
                    for cell in row[:3]
                    if (
                        candidate := _labour_cost_economy_cell(
                            cell,
                            expected_ea_composition=expected_ea_composition,
                        )
                    )
                    is not None
                ),
                None,
            )
            if economy is None:
                continue
            for column, (period, measure) in columns.items():
                if (
                    not minimum_period <= period <= reference_period
                    or column >= len(row)
                ):
                    continue
                lexical = _labour_cost_value_cell(row[column])
                if lexical is None:
                    continue
                item = EurostatLabourCostPublishedValueV1(
                    economy_code=economy,
                    reference_period=period,
                    measure=measure,
                    value_lexical=lexical,
                    value=float(lexical),
                    evidence_artifact_sha256=artifact.content_sha256,
                    evidence_locator=(
                        f"labour-cost HTML table {table_number}; "
                        f"{economy} row; {period}; {measure.value}"
                    ),
                )
                previous = values.get((economy, period, measure))
                if previous is not None and previous.value != item.value:
                    raise ValueError(
                        "Eurostat labour cost HTML tables conflict for "
                        f"{artifact.product_code}: {economy} {period} {measure.value}"
                    )
                values.setdefault((economy, period, measure), item)
    return tuple(
        sorted(
            values.values(),
            key=lambda item: (
                item.reference_period,
                _RELEASE_ECONOMY_ORDER[item.economy_code],
                list(EurostatLabourCostMeasure).index(item.measure),
            ),
        )
    )


def _labour_cost_pdf_page_values(
    page_text: str,
    artifact: EurostatLabourCostArtifactV1,
    reference_period: str,
    page_number: int,
) -> tuple[EurostatLabourCostPublishedValueV1, ...]:
    """Decode source rows from one text-bearing release PDF page."""
    normalized = page_text.replace("\u2212", "-")
    normalized = re.sub(r"(?<=\d)\s+\.(?=\d)", ".", normalized)
    normalized = re.sub(r"(?<=\d)\.\s+(?=\d(?:\s|$))", ".", normalized)
    normalized = re.sub(r"(?<!\w)-\s+(?=\d)", "-", normalized)
    lines = tuple(
        line.strip() for line in normalized.splitlines() if line.strip()
    )
    start = next(
        (
            index
            for index, line in enumerate(lines)
            if "labour cost" in line.casefold()
            and (
                "whole economy" in line.casefold()
                or "industry and services" in line.casefold()
                or line.casefold().startswith("nominal hourly labour cost")
            )
        ),
        None,
    )
    if start is None:
        return ()
    section_end = next(
        (
            index
            for index in range(start + 1, len(lines))
            if "labour cost" in lines[index].casefold()
            and any(
                marker in lines[index].casefold()
                for marker in (
                    "business economy",
                    "non-business economy",
                    "industry ",
                    "construction",
                    "services",
                )
            )
        ),
        len(lines),
    )
    section = lines[start:section_end]
    period_columns = tuple(
        dict.fromkeys(
            period for line in section for period in _labour_cost_periods(line)
        )
    )
    correction = EUROSTAT_LABOUR_COST_PERIOD_HEADER_CORRECTIONS.get(
        cast(str, artifact.product_code)
    )
    if correction is not None and period_columns == correction[0]:
        period_columns = correction[1]
    if (
        reference_period not in period_columns
        or not 1 <= len(period_columns) <= 8
    ):
        return ()
    measures = tuple(
        dict.fromkeys(
            measure
            for line in section
            if (measure := _labour_cost_measure_cell(line)) is not None
        )
    )
    if set(measures) != set(EurostatLabourCostMeasure):
        header = " ".join(section[:12]).casefold()
        measures = tuple(
            measure
            for measure, markers in (
                (EurostatLabourCostMeasure.TOTAL, ("total",)),
                (EurostatLabourCostMeasure.WAGES, ("wage", "salar", "wag")),
                (EurostatLabourCostMeasure.OTHER, ("other", "non-wage", "oth")),
            )
            if any(marker in header for marker in markers)
        )
    if set(measures) != set(EurostatLabourCostMeasure):
        return ()
    columns = tuple(
        (period, measure) for period in period_columns for measure in measures
    )
    values: dict[
        tuple[str, str, EurostatLabourCostMeasure],
        EurostatLabourCostPublishedValueV1,
    ] = {}
    minimum_period = _quarter_shift(reference_period, -12)
    expected_ea_composition = _ea_composition_for_period(reference_period)
    for line_index, line in enumerate(section):
        row_match = _LABOUR_COST_PDF_ROW_RE.match(line)
        if row_match is None:
            continue
        economy = _labour_cost_economy_cell(
            row_match.group("label"),
            expected_ea_composition=expected_ea_composition,
        )
        if economy is None:
            continue
        body_parts = [row_match.group("body").strip()]
        if len(tuple(_LABOUR_COST_PDF_NUMBER_RE.finditer(body_parts[0]))) < len(
            columns
        ):
            for continuation in section[line_index + 1 : line_index + 5]:
                if _LABOUR_COST_PDF_ROW_RE.match(continuation) is not None:
                    break
                body_parts.append(continuation)
                if len(
                    tuple(
                        _LABOUR_COST_PDF_NUMBER_RE.finditer(
                            " ".join(body_parts)
                        )
                    )
                ) >= len(columns):
                    break
        body = " ".join(body_parts)
        tokens = tuple(
            match.group(0).replace("\u2212", "-")
            for match in _LABOUR_COST_PDF_NUMBER_RE.finditer(body)
        )
        if len(tokens) != len(columns):
            continue
        for (period, measure), lexical in zip(columns, tokens, strict=True):
            if (
                lexical == ":"
                or not minimum_period <= period <= reference_period
            ):
                continue
            item = EurostatLabourCostPublishedValueV1(
                economy_code=economy,
                reference_period=period,
                measure=measure,
                value_lexical=lexical,
                value=float(lexical),
                evidence_artifact_sha256=artifact.content_sha256,
                evidence_locator=(
                    f"labour-cost PDF page {page_number}; "
                    f"{economy} row; {period}; {measure.value}"
                ),
            )
            key = (economy, period, measure)
            previous = values.get(key)
            if previous is not None and previous.value != item.value:
                raise ValueError(
                    "Eurostat labour cost PDF tables conflict for "
                    f"{artifact.product_code}: {economy} {period} {measure.value}"
                )
            values.setdefault(key, item)
    return tuple(values.values())


def _labour_cost_pdf_table_values(
    snapshot: OfficialRawSnapshotV1,
    artifact: EurostatLabourCostArtifactV1,
    reference_period: str,
) -> tuple[EurostatLabourCostPublishedValueV1, ...]:
    if snapshot.request.source_format is not OfficialSourceFormat.PDF:
        return ()
    try:
        reader = PdfReader(BytesIO(snapshot.content), strict=False)
    except PdfReadError as exc:
        raise ValueError(
            "Eurostat labour cost release PDF table extraction failed"
        ) from exc
    values: dict[
        tuple[str, str, EurostatLabourCostMeasure],
        EurostatLabourCostPublishedValueV1,
    ] = {}
    failed_pages: list[int] = []
    readable_page_count = 0
    for page_number, page in enumerate(reader.pages, start=1):
        try:
            page_text = page.extract_text() or ""
        except PdfReadError:
            failed_pages.append(page_number)
            if len(failed_pages) > 1:
                raise ValueError(
                    "Eurostat labour cost release PDF table has too many "
                    "unreadable pages"
                )
            continue
        if page_text.strip():
            readable_page_count += 1
        for item in _labour_cost_pdf_page_values(
            page_text, artifact, reference_period, page_number
        ):
            key = (item.economy_code, item.reference_period, item.measure)
            previous = values.get(key)
            if previous is not None and previous.value != item.value:
                raise ValueError(
                    "Eurostat labour cost PDF pages conflict for "
                    f"{artifact.product_code}: {item.economy_code} "
                    f"{item.reference_period} {item.measure.value}"
                )
            values.setdefault(key, item)
    if not readable_page_count:
        raise ValueError(
            "Eurostat labour cost release PDF table has no readable text"
        )
    return tuple(
        sorted(
            values.values(),
            key=lambda item: (
                item.reference_period,
                _RELEASE_ECONOMY_ORDER[item.economy_code],
                list(EurostatLabourCostMeasure).index(item.measure),
            ),
        )
    )


def _labour_cost_table_published_values(
    snapshot: OfficialRawSnapshotV1,
    artifact: EurostatLabourCostArtifactV1,
    reference_period: str,
) -> tuple[EurostatLabourCostPublishedValueV1, ...]:
    values = _labour_cost_html_table_values(
        snapshot, artifact, reference_period
    )
    if values:
        return values
    values = _labour_cost_source_table_values(
        snapshot, artifact, reference_period
    )
    if values:
        return values
    return _labour_cost_pdf_table_values(snapshot, artifact, reference_period)


def _published_values(
    landing_snapshot: OfficialRawSnapshotV1,
    landing_artifact: EurostatLabourCostArtifactV1,
    document_snapshot: OfficialRawSnapshotV1 | None,
    document_artifact: EurostatLabourCostArtifactV1 | None,
    reference_period: str,
    headline: EurostatLabourCostReleaseValueV1,
) -> tuple[EurostatLabourCostPublishedValueV1, ...]:
    parsed: tuple[EurostatLabourCostPublishedValueV1, ...] = ()
    for snapshot, artifact in (
        (document_snapshot, document_artifact),
        (landing_snapshot, landing_artifact),
    ):
        if snapshot is None or artifact is None:
            continue
        parsed = _labour_cost_table_published_values(
            snapshot, artifact, reference_period
        )
        if parsed:
            break
    values = list(parsed)
    headline_match = next(
        (
            item
            for item in values
            if item.economy_code == "EA"
            and item.measure is EurostatLabourCostMeasure.TOTAL
            and item.reference_period == reference_period
        ),
        None,
    )
    if headline_match is None:
        values.append(
            EurostatLabourCostPublishedValueV1(
                economy_code="EA",
                reference_period=reference_period,
                measure=EurostatLabourCostMeasure.TOTAL,
                value_lexical=(
                    f"-{headline.value_lexical}"
                    if headline.movement is EurostatLabourCostMovement.DOWN
                    else headline.value_lexical
                ),
                value=headline.value,
                evidence_artifact_sha256=landing_artifact.content_sha256,
                evidence_locator="source-authored release headline",
            )
        )
    elif headline_match.value != headline.value:
        raise ValueError(
            "Eurostat labour cost headline and publication table differ for "
            f"{landing_artifact.product_code}: headline={headline.value}, "
            f"table={headline_match.value}"
        )
    values.sort(
        key=lambda item: (
            item.reference_period,
            _RELEASE_ECONOMY_ORDER[item.economy_code],
            list(EurostatLabourCostMeasure).index(item.measure),
        )
    )
    return tuple(values)


@dataclass(frozen=True, slots=True)
class EurostatLabourCostReleaseV1:
    """One source-dated quarterly labour-cost release."""

    inventory_entry: EurostatLabourCostInventoryEntryV1
    reference_period: str
    reference_period_source_stated: bool
    days_after_period_end: int
    euro_area_composition: str
    composition_source_stated: bool
    activity_scope: EurostatLabourCostActivityScope
    page_release_date: str
    page_release_date_offset_days: int
    published_at: str | None
    linked_document_uris: tuple[str, ...]
    artifact: EurostatLabourCostArtifactV1
    document_artifact: EurostatLabourCostArtifactV1 | None
    headline_value: EurostatLabourCostReleaseValueV1
    published_values: tuple[EurostatLabourCostPublishedValueV1, ...]
    release_id: str = ""
    schema_version: str = EUROSTAT_LABOUR_COST_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_LABOUR_COST_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat labour cost release schema")
        reference = _quarter(self.reference_period, "reference_period")
        if not isinstance(self.reference_period_source_stated, bool):
            raise TypeError("reference_period_source_stated must be boolean")
        inferred_reference = (
            EUROSTAT_LABOUR_COST_REFERENCE_PERIOD_INFERENCES.get(
                self.inventory_entry.product_code
            )
        )
        if (
            inferred_reference is None
            and not self.reference_period_source_stated
        ) or (
            inferred_reference is not None
            and (
                self.reference_period_source_stated
                or reference != inferred_reference
            )
        ):
            raise ValueError(
                "Eurostat labour cost reference-period evidence differs"
            )
        expected_lag = _release_lag(
            self.inventory_entry.release_date, reference
        )
        if self.days_after_period_end != expected_lag:
            raise ValueError(
                "Eurostat labour cost release period or lag differs"
            )
        composition = _required_text(
            self.euro_area_composition, "euro_area_composition"
        )
        if composition != _ea_composition_for_period(reference):
            raise ValueError("Eurostat labour cost EA composition differs")
        if not isinstance(self.composition_source_stated, bool):
            raise TypeError("composition_source_stated must be boolean")
        activity_scope = EurostatLabourCostActivityScope.from_value(
            self.activity_scope
        )
        if activity_scope is not _activity_scope_for_period(reference):
            raise ValueError("Eurostat labour cost activity scope differs")
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
            EUROSTAT_LABOUR_COST_PAGE_DATE_OFFSET_EXCEPTIONS.get(
                self.inventory_entry.product_code
            )
        )
        if self.page_release_date_offset_days != offset or (
            (expected_exception is None and offset != 0)
            or (expected_exception is not None and offset != expected_exception)
        ):
            raise ValueError(
                "Eurostat labour cost landing date offset differs for "
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
                    "Eurostat labour cost publication timestamp date differs"
                )
        links = tuple(
            _https_uri(item, "linked_document_uri")
            for item in self.linked_document_uris
        )
        if links != tuple(sorted(set(links))):
            raise ValueError(
                "Eurostat labour cost release document links are not canonical"
            )
        if len(links) > 1:
            raise ValueError(
                "Eurostat labour cost release has multiple English documents"
            )
        if any(
            _product_code(item) != self.inventory_entry.product_code
            for item in links
        ):
            raise ValueError(
                "Eurostat labour cost release document product code differs"
            )
        if (
            self.artifact.role
            is not EurostatLabourCostArtifactRole.RELEASE_HTML
        ):
            raise ValueError(
                "Eurostat labour cost release artifact role differs"
            )
        if (
            self.artifact.product_code != self.inventory_entry.product_code
            or self.artifact.request_uri != self.inventory_entry.source_uri
            or _product_code(self.artifact.resolved_uri)
            != self.inventory_entry.product_code
        ):
            raise ValueError(
                "Eurostat labour cost release artifact product code differs"
            )
        document = self.document_artifact
        if (document is None) != (not links):
            raise ValueError(
                "Eurostat labour cost release document artifact differs"
            )
        if document is not None and (
            document.role
            not in {
                EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_HTML,
                EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_PDF,
            }
            or document.request_uri != links[0]
            or document.product_code != self.inventory_entry.product_code
            or _product_code(document.resolved_uri)
            != self.inventory_entry.product_code
        ):
            raise ValueError(
                "Eurostat labour cost release document identity differs"
            )
        value = self.headline_value
        if (
            value.reference_period != reference
            or value.evidence_id != self.inventory_entry.entry_id
        ):
            raise ValueError(
                "Eurostat labour cost release headline occurrence differs"
            )
        published_values = tuple(self.published_values)
        if not (
            1
            <= len(published_values)
            <= MAX_EUROSTAT_LABOUR_COST_PUBLISHED_VALUES_PER_RELEASE
        ):
            raise ValueError(
                "Eurostat labour cost published-value count differs"
            )
        keys = [item.key for item in published_values]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "Eurostat labour cost release repeats a published value"
            )
        if list(published_values) != sorted(
            published_values,
            key=lambda item: (
                item.reference_period,
                _RELEASE_ECONOMY_ORDER[item.economy_code],
                list(EurostatLabourCostMeasure).index(item.measure),
            ),
        ):
            raise ValueError(
                "Eurostat labour cost published values are not canonical"
            )
        evidence_digests = {self.artifact.content_sha256}
        if document is not None:
            evidence_digests.add(document.content_sha256)
        if any(
            item.reference_period > reference
            or item.reference_period < _quarter_shift(reference, -12)
            or item.evidence_artifact_sha256 not in evidence_digests
            for item in published_values
        ):
            raise ValueError(
                "Eurostat labour cost published-value evidence differs"
            )
        headline_published = next(
            (
                item
                for item in published_values
                if item.economy_code == "EA"
                and item.measure is EurostatLabourCostMeasure.TOTAL
                and item.reference_period == reference
            ),
            None,
        )
        if (
            headline_published is None
            or headline_published.value != value.value
        ):
            raise ValueError(
                "Eurostat labour cost headline published value differs"
            )
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "euro_area_composition", composition)
        object.__setattr__(self, "activity_scope", activity_scope)
        object.__setattr__(self, "page_release_date", page_date)
        object.__setattr__(self, "published_at", published_at)
        object.__setattr__(self, "linked_document_uris", links)
        object.__setattr__(self, "published_values", published_values)
        expected = _stable_id(
            "eurostat-labour-cost-release", self.identity_payload()
        )
        if self.release_id and self.release_id != expected:
            raise ValueError("Eurostat labour cost release identity differs")
        object.__setattr__(self, "release_id", expected)

    @property
    def occurrence_key(self) -> tuple[str, str]:
        return self.reference_period, self.inventory_entry.product_code

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "inventory_entry": self.inventory_entry.to_dict(),
            "reference_period": self.reference_period,
            "reference_period_source_stated": (
                self.reference_period_source_stated
            ),
            "days_after_period_end": self.days_after_period_end,
            "euro_area_composition": self.euro_area_composition,
            "composition_source_stated": self.composition_source_stated,
            "activity_scope": self.activity_scope.value,
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
    def from_dict(cls, data: Mapping[str, Any]) -> EurostatLabourCostReleaseV1:
        return cls(
            inventory_entry=EurostatLabourCostInventoryEntryV1.from_dict(
                _mapping(data.get("inventory_entry"), "inventory_entry")
            ),
            reference_period=str(data.get("reference_period", "")),
            reference_period_source_stated=cast(
                bool, data.get("reference_period_source_stated")
            ),
            days_after_period_end=cast(int, data.get("days_after_period_end")),
            euro_area_composition=str(data.get("euro_area_composition", "")),
            composition_source_stated=cast(
                bool, data.get("composition_source_stated")
            ),
            activity_scope=EurostatLabourCostActivityScope.from_value(
                str(data.get("activity_scope", ""))
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
            artifact=EurostatLabourCostArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            document_artifact=(
                EurostatLabourCostArtifactV1.from_dict(
                    _mapping(data.get("document_artifact"), "document_artifact")
                )
                if data.get("document_artifact") is not None
                else None
            ),
            headline_value=EurostatLabourCostReleaseValueV1.from_dict(
                _mapping(data.get("headline_value"), "headline_value")
            ),
            published_values=tuple(
                EurostatLabourCostPublishedValueV1.from_dict(
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
    releases: Sequence[EurostatLabourCostReleaseV1],
) -> tuple[int, int]:
    series: dict[tuple[str, str, str, str], list[float]] = {}
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
                release.activity_scope.value,
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
    releases: Sequence[EurostatLabourCostReleaseV1],
    dataset: EurostatLabourCostDatasetV1,
) -> tuple[int, int]:
    current = {
        (item.economy_code, item.reference_period, item.measure): item.value
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
            revised = current.get(
                (economy, value.reference_period, value.measure)
            )
            if revised is None:
                continue
            comparison_count += 1
            changed_count += revised != value.value
    return comparison_count, changed_count


@dataclass(frozen=True, slots=True)
class EurostatLabourCostArchiveManifestV1:
    """Compact, replayable receipt for the official labour cost corpus."""

    as_of_date: str
    registry_id: str
    source_id: str
    inventory: EurostatLabourCostInventoryV1
    current_dataset: EurostatLabourCostDatasetV1
    releases: tuple[EurostatLabourCostReleaseV1, ...]
    searchable_release_gap_reference_periods: tuple[str, ...]
    reference_period_inferred_count: int
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
    schema_version: str = EUROSTAT_LABOUR_COST_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EUROSTAT_LABOUR_COST_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported Eurostat labour cost manifest schema")
        as_of = _iso_date(self.as_of_date, "as_of_date")
        if as_of != EUROSTAT_LABOUR_COST_AS_OF_DATE:
            raise ValueError("Eurostat labour cost archive boundary differs")
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError(
                "Eurostat labour cost registry identity is invalid"
            )
        if not source_id.startswith("official-source:sha256:"):
            raise ValueError("Eurostat labour cost source identity is invalid")
        releases = tuple(self.releases)
        if len(releases) != EUROSTAT_LABOUR_COST_RELEASE_COUNT:
            raise ValueError(
                "Eurostat labour cost manifest release count differs"
            )
        if list(releases) != sorted(
            releases,
            key=lambda item: (
                item.inventory_entry.release_date,
                item.inventory_entry.product_code,
            ),
        ):
            raise ValueError("Eurostat labour cost releases are not canonical")
        if [item.inventory_entry for item in releases] != list(
            self.inventory.entries
        ):
            raise ValueError(
                "Eurostat labour cost release and inventory entries differ"
            )
        if any(
            item.headline_value
            != _headline_value(item.inventory_entry, item.reference_period)
            for item in releases
        ):
            raise ValueError(
                "Eurostat labour cost release headline evidence differs"
            )
        occurrences = [item.occurrence_key for item in releases]
        if len(occurrences) != len(set(occurrences)):
            raise ValueError(
                "Eurostat labour cost releases repeat an occurrence"
            )
        periods = [item.reference_period for item in releases]
        if len(periods) != len(set(periods)):
            raise ValueError("Eurostat labour cost releases repeat a quarter")
        if (
            min(periods) != EUROSTAT_LABOUR_COST_FIRST_REFERENCE_PERIOD
            or max(periods) != EUROSTAT_LABOUR_COST_LATEST_REFERENCE_PERIOD
        ):
            raise ValueError("Eurostat labour cost reference range differs")
        expected_release_periods = tuple(
            item
            for item in _quarter_range(
                EUROSTAT_LABOUR_COST_FIRST_REFERENCE_PERIOD,
                EUROSTAT_LABOUR_COST_LATEST_REFERENCE_PERIOD,
            )
            if item not in EUROSTAT_LABOUR_COST_INTERNAL_GAP_REFERENCE_PERIODS
        )
        if tuple(periods) != expected_release_periods:
            raise ValueError(
                "Eurostat labour cost reference-quarter lineage differs"
            )
        if any(item.inventory_entry.release_date > as_of for item in releases):
            raise ValueError(
                "Eurostat labour cost release exceeds archive boundary"
            )
        if self.current_dataset.updated_at[:10] > as_of:
            raise ValueError(
                "Eurostat labour cost dataset exceeds archive boundary"
            )
        expected_gaps = (
            *_quarter_range(
                self.current_dataset.time_start,
                _quarter_shift(EUROSTAT_LABOUR_COST_FIRST_REFERENCE_PERIOD, -1),
            ),
            *EUROSTAT_LABOUR_COST_INTERNAL_GAP_REFERENCE_PERIODS,
        )
        gaps = tuple(
            _quarter(item, "searchable_release_gap_reference_period")
            for item in self.searchable_release_gap_reference_periods
        )
        if gaps != expected_gaps:
            raise ValueError(
                "Eurostat labour cost searchable-release gap inventory differs"
            )
        for name, maximum in (
            (
                "reference_period_inferred_count",
                MAX_EUROSTAT_LABOUR_COST_RELEASES,
            ),
            (
                "composition_source_stated_count",
                MAX_EUROSTAT_LABOUR_COST_RELEASES,
            ),
            (
                "page_date_offset_count",
                MAX_EUROSTAT_LABOUR_COST_RELEASES,
            ),
            (
                "exact_publication_time_count",
                MAX_EUROSTAT_LABOUR_COST_RELEASES,
            ),
            (
                "release_document_count",
                MAX_EUROSTAT_LABOUR_COST_RELEASES,
            ),
            (
                "published_value_count",
                MAX_EUROSTAT_LABOUR_COST_RELEASES
                * MAX_EUROSTAT_LABOUR_COST_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "revision_comparison_count",
                MAX_EUROSTAT_LABOUR_COST_RELEASES
                * MAX_EUROSTAT_LABOUR_COST_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "changed_revision_count",
                MAX_EUROSTAT_LABOUR_COST_RELEASES
                * MAX_EUROSTAT_LABOUR_COST_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "current_dataset_comparison_count",
                MAX_EUROSTAT_LABOUR_COST_RELEASES
                * MAX_EUROSTAT_LABOUR_COST_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "changed_current_dataset_count",
                MAX_EUROSTAT_LABOUR_COST_RELEASES
                * MAX_EUROSTAT_LABOUR_COST_PUBLISHED_VALUES_PER_RELEASE,
            ),
        ):
            _bounded_int(getattr(self, name), name, maximum)
        expected_inferred_references = sum(
            not item.reference_period_source_stated for item in releases
        )
        if self.reference_period_inferred_count != expected_inferred_references:
            raise ValueError(
                "Eurostat labour cost inferred-reference count differs"
            )
        if self.composition_source_stated_count != sum(
            item.composition_source_stated for item in releases
        ):
            raise ValueError("Eurostat labour cost composition count differs")
        expected_offsets = sum(
            item.page_release_date_offset_days != 0 for item in releases
        )
        if self.page_date_offset_count != expected_offsets:
            raise ValueError(
                "Eurostat labour cost page-date offset count differs"
            )
        expected_exact_times = sum(
            item.published_at is not None for item in releases
        )
        if self.exact_publication_time_count != expected_exact_times:
            raise ValueError(
                "Eurostat labour cost exact publication-time count differs"
            )
        document_artifacts = tuple(
            item.document_artifact
            for item in releases
            if item.document_artifact is not None
        )
        if len(document_artifacts) != EUROSTAT_LABOUR_COST_DOCUMENT_COUNT:
            raise ValueError(
                "Eurostat labour cost English document count differs"
            )
        if self.release_document_count != len(document_artifacts):
            raise ValueError(
                "Eurostat labour cost release-document count differs"
            )
        expected_value_count = sum(
            len(item.published_values) for item in releases
        )
        if self.published_value_count != expected_value_count:
            raise ValueError(
                "Eurostat labour cost published-value count differs"
            )
        revision_counts = _published_revision_counts(releases)
        if revision_counts != (
            self.revision_comparison_count,
            self.changed_revision_count,
        ):
            raise ValueError("Eurostat labour cost revision counts differ")
        dataset_counts = _current_dataset_comparison_counts(
            releases, self.current_dataset
        )
        if dataset_counts != (
            self.current_dataset_comparison_count,
            self.changed_current_dataset_count,
        ):
            raise ValueError("Eurostat labour cost dataset comparison differs")
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
                MAX_EUROSTAT_LABOUR_COST_ARTIFACTS,
            ),
            _bounded_int(
                self.unique_content_sha256_count,
                "unique_content_sha256_count",
                MAX_EUROSTAT_LABOUR_COST_ARTIFACTS,
            ),
            _bounded_int(
                self.total_content_bytes,
                "total_content_bytes",
                MAX_EUROSTAT_LABOUR_COST_TOTAL_BYTES,
            ),
        )
        if counts != (
            expected_artifact_count,
            expected_unique_count,
            expected_bytes,
        ):
            raise ValueError("Eurostat labour cost artifact totals differ")
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "releases", releases)
        object.__setattr__(
            self, "searchable_release_gap_reference_periods", gaps
        )
        expected = _stable_id(
            "eurostat-labour-cost-archive-manifest",
            self.identity_payload(),
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("Eurostat labour cost manifest identity differs")
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
            "reference_period_inferred_count": (
                self.reference_period_inferred_count
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
    ) -> EurostatLabourCostArchiveManifestV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            inventory=EurostatLabourCostInventoryV1.from_dict(
                _mapping(data.get("inventory"), "inventory")
            ),
            current_dataset=EurostatLabourCostDatasetV1.from_dict(
                _mapping(data.get("current_dataset"), "current_dataset")
            ),
            releases=tuple(
                EurostatLabourCostReleaseV1.from_dict(_mapping(item, "release"))
                for item in _sequence(data.get("releases"), "releases")
            ),
            searchable_release_gap_reference_periods=tuple(
                str(item)
                for item in _sequence(
                    data.get("searchable_release_gap_reference_periods"),
                    "searchable_release_gap_reference_periods",
                )
            ),
            reference_period_inferred_count=cast(
                int, data.get("reference_period_inferred_count")
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
    def from_json(cls, value: str) -> EurostatLabourCostArchiveManifestV1:
        try:
            data = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "Eurostat labour cost manifest is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_eurostat_labour_cost_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    dataset_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatLabourCostArchiveManifestV1:
    """Build the labour cost receipt from exact retained Eurostat bytes."""
    source = registry.source(EUROSTAT_LABOUR_COST_SOURCE_KEY)
    if (
        source.parser_id != EUROSTAT_LABOUR_COST_PARSER_ID
        or source.parser_version != EUROSTAT_LABOUR_COST_PARSER_VERSION
    ):
        raise ValueError(
            "Eurostat labour cost registry parser identity differs"
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
            "Eurostat labour cost snapshot source identity differs"
        )
    inventory = parse_eurostat_labour_cost_inventory(
        search_values, as_of_date=as_of_date
    )
    dataset = parse_eurostat_labour_cost_dataset(dataset_snapshot)
    snapshots = release_values
    if len(snapshots) != EUROSTAT_LABOUR_COST_RELEASE_COUNT:
        raise ValueError("Eurostat labour cost release snapshot count differs")
    by_code: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        code = _product_code(snapshot.request.uri)
        if code in by_code:
            raise ValueError(
                "Eurostat labour cost release corpus repeats a product code"
            )
        by_code[code] = snapshot
    if set(by_code) != {item.product_code for item in inventory.entries}:
        raise ValueError("Eurostat labour cost release corpus is incomplete")
    if len(document_values) != EUROSTAT_LABOUR_COST_DOCUMENT_COUNT:
        raise ValueError("Eurostat labour cost document snapshot count differs")
    documents_by_uri: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in document_values:
        if snapshot.request.uri in documents_by_uri:
            raise ValueError(
                "Eurostat labour cost document corpus repeats a URI"
            )
        documents_by_uri[snapshot.request.uri] = snapshot
    used_document_uris: set[str] = set()
    parsed: list[
        tuple[
            EurostatLabourCostInventoryEntryV1,
            EurostatLabourCostArtifactV1,
            EurostatLabourCostArtifactV1 | None,
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
                "Eurostat labour cost release request identity differs"
            )
        artifact = _artifact_from_snapshot(
            snapshot,
            EurostatLabourCostArtifactRole.RELEASE_HTML,
            product_code=entry.product_code,
        )
        _, page_date, published_at, document_uris = _parse_release_landing(
            snapshot, entry
        )
        document_artifact: EurostatLabourCostArtifactV1 | None = None
        document_snapshot: OfficialRawSnapshotV1 | None = None
        if document_uris:
            document_uri = document_uris[0]
            try:
                document_snapshot = documents_by_uri[document_uri]
            except KeyError as exc:
                raise ValueError(
                    "Eurostat labour cost document corpus is incomplete"
                ) from exc
            used_document_uris.add(document_uri)
            document_role = {
                OfficialSourceFormat.HTML: (
                    EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_HTML
                ),
                OfficialSourceFormat.PDF: (
                    EurostatLabourCostArtifactRole.RELEASE_DOCUMENT_PDF
                ),
            }.get(document_snapshot.request.source_format)
            if document_role is None:
                raise ValueError("Eurostat labour cost document format differs")
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
            "Eurostat labour cost document corpus has unexpected artifacts"
        )
    releases: list[EurostatLabourCostReleaseV1] = []
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
                "Eurostat labour cost release lag changed during parsing"
            )
        headline = _headline_value(entry, reference_period)
        document_snapshot = (
            documents_by_uri[document_uris[0]] if document_uris else None
        )
        releases.append(
            EurostatLabourCostReleaseV1(
                inventory_entry=entry,
                reference_period=reference_period,
                reference_period_source_stated=(
                    entry.product_code
                    not in EUROSTAT_LABOUR_COST_REFERENCE_PERIOD_INFERENCES
                ),
                days_after_period_end=lag,
                euro_area_composition=composition,
                composition_source_stated=composition_source_stated,
                activity_scope=_activity_scope_for_period(reference_period),
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
    return EurostatLabourCostArchiveManifestV1(
        as_of_date=as_of_date,
        registry_id=registry.registry_id,
        source_id=source.source_id,
        inventory=inventory,
        current_dataset=dataset,
        releases=tuple(releases),
        searchable_release_gap_reference_periods=(
            *_quarter_range(
                dataset.time_start,
                _quarter_shift(EUROSTAT_LABOUR_COST_FIRST_REFERENCE_PERIOD, -1),
            ),
            *EUROSTAT_LABOUR_COST_INTERNAL_GAP_REFERENCE_PERIODS,
        ),
        reference_period_inferred_count=sum(
            not item.reference_period_source_stated for item in releases
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


def replay_eurostat_labour_cost_archive(
    registry: OfficialSourceRegistryV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    dataset_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Sequence[OfficialRawSnapshotV1],
    expected_manifest: EurostatLabourCostArchiveManifestV1,
) -> EurostatLabourCostArchiveManifestV1:
    """Rebuild the labour cost receipt and require byte-for-byte identity."""
    rebuilt = build_eurostat_labour_cost_archive_manifest(
        registry,
        search_snapshots,
        dataset_snapshot,
        release_snapshots,
        document_snapshots,
        as_of_date=expected_manifest.as_of_date,
    )
    if rebuilt.to_json() != expected_manifest.to_json():
        raise ValueError(
            "Eurostat labour cost replay differs from packaged manifest"
        )
    return rebuilt


def packaged_eurostat_labour_cost_manifest_path() -> Path:
    """Return the installed labour cost archive-manifest path."""
    return (
        Path(__file__).resolve().parent
        / "assets"
        / "eurostat_labour_cost_archive_v1.json"
    )


def load_packaged_eurostat_labour_cost_archive_manifest() -> (
    EurostatLabourCostArchiveManifestV1
):
    """Load and validate the installed labour cost archive manifest."""
    path = packaged_eurostat_labour_cost_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError(
            "packaged Eurostat labour cost manifest exceeds size bound"
        )
    manifest = EurostatLabourCostArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_LABOUR_COST_SOURCE_KEY)
    if (
        manifest.registry_id != registry.registry_id
        or manifest.source_id != source.source_id
    ):
        raise ValueError(
            "packaged Eurostat labour cost registry binding differs"
        )
    return manifest


__all__ = [
    "EUROSTAT_LABOUR_COST_ARTIFACT_SCHEMA_VERSION",
    "EUROSTAT_LABOUR_COST_AS_OF_DATE",
    "EUROSTAT_LABOUR_COST_DATASET_SCHEMA_VERSION",
    "EUROSTAT_LABOUR_COST_DATASET_URI",
    "EUROSTAT_LABOUR_COST_DOCUMENT_COUNT",
    "EUROSTAT_LABOUR_COST_EXCLUSION_SCHEMA_VERSION",
    "EUROSTAT_LABOUR_COST_FIRST_REFERENCE_PERIOD",
    "EUROSTAT_LABOUR_COST_FIRST_SEARCH_RELEASE_DATE",
    "EUROSTAT_LABOUR_COST_INTERNAL_GAP_REFERENCE_PERIODS",
    "EUROSTAT_LABOUR_COST_INVENTORY_ENTRY_SCHEMA_VERSION",
    "EUROSTAT_LABOUR_COST_INVENTORY_SCHEMA_VERSION",
    "EUROSTAT_LABOUR_COST_LATEST_PACKAGED_RELEASE_DATE",
    "EUROSTAT_LABOUR_COST_LATEST_REFERENCE_PERIOD",
    "EUROSTAT_LABOUR_COST_MANIFEST_SCHEMA_VERSION",
    "EUROSTAT_LABOUR_COST_MEASURE_KEY",
    "EUROSTAT_LABOUR_COST_OBSERVATION_SCHEMA_VERSION",
    "EUROSTAT_LABOUR_COST_PAGE_DATE_OFFSET_EXCEPTIONS",
    "EUROSTAT_LABOUR_COST_PARSER_ID",
    "EUROSTAT_LABOUR_COST_PARSER_VERSION",
    "EUROSTAT_LABOUR_COST_PERIOD_HEADER_CORRECTIONS",
    "EUROSTAT_LABOUR_COST_PROGRAM_KEY",
    "EUROSTAT_LABOUR_COST_PUBLISHED_VALUE_SCHEMA_VERSION",
    "EUROSTAT_LABOUR_COST_REFERENCE_PERIOD_INFERENCES",
    "EUROSTAT_LABOUR_COST_RELEASE_COUNT",
    "EUROSTAT_LABOUR_COST_RELEASE_SCHEMA_VERSION",
    "EUROSTAT_LABOUR_COST_RELEASE_VALUE_SCHEMA_VERSION",
    "EUROSTAT_LABOUR_COST_SEARCH_PAGE_COUNT",
    "EUROSTAT_LABOUR_COST_SEARCH_RESULT_COUNT",
    "EUROSTAT_LABOUR_COST_SEARCH_UNIQUE_URI_COUNT",
    "EUROSTAT_LABOUR_COST_SEARCH_URI",
    "EUROSTAT_LABOUR_COST_SOURCE_KEY",
    "EUROSTAT_LABOUR_COST_SOURCE_TIMEZONE",
    "EurostatLabourCostArchiveManifestV1",
    "EurostatLabourCostArtifactRole",
    "EurostatLabourCostArtifactV1",
    "EurostatLabourCostDatasetV1",
    "EurostatLabourCostIndexExclusionV1",
    "EurostatLabourCostInventoryEntryV1",
    "EurostatLabourCostInventoryV1",
    "EurostatLabourCostMeasure",
    "EurostatLabourCostMovement",
    "EurostatLabourCostObservationV1",
    "EurostatLabourCostPublishedValueV1",
    "EurostatLabourCostReleaseV1",
    "EurostatLabourCostReleaseValueV1",
    "build_eurostat_labour_cost_archive_manifest",
    "build_eurostat_labour_cost_dataset_request",
    "build_eurostat_labour_cost_document_requests",
    "build_eurostat_labour_cost_release_requests",
    "build_eurostat_labour_cost_search_requests",
    "load_packaged_eurostat_labour_cost_archive_manifest",
    "packaged_eurostat_labour_cost_manifest_path",
    "parse_eurostat_labour_cost_dataset",
    "parse_eurostat_labour_cost_inventory",
    "replay_eurostat_labour_cost_archive",
]
