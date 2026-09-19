"""Deterministic qualification of Eurostat monthly international trade in goods releases.

Eurostat's searchable Euro-indicator feed is the release inventory, while
source-dated HTML/PDF products are the vintage evidence.  The current
``ext_st_easitc`` JSON-stat table is retained only as a revised-series cross-check;
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
from functools import lru_cache
from html.parser import HTMLParser
from io import BytesIO
from itertools import chain, pairwise
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlsplit
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

EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY: Final = (
    "ea.eurostat.international-trade-goods"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_PROGRAM_KEY: Final = (
    "ea.eurostat.monthly-international-trade-goods"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_MEASURE_KEY: Final = (
    "international-trade-goods-value-total"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_CURRENT_DATASET_UNIT: Final = "million-euro"
EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_UNIT: Final = "billion-euro"
EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_LABEL: Final = (
    "Euro area trade by SITC product group"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_ID: Final = (
    "official.eurostat-international-trade-goods.v1"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_VERSION: Final = "1"
EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_TIMEZONE: Final = "Europe/Brussels"
EUROSTAT_INTERNATIONAL_TRADE_GOODS_AS_OF_DATE: Final = "2026-09-18"
EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_SEARCH_RELEASE_DATE: Final = (
    "2012-06-15"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_PACKAGED_RELEASE_DATE: Final = (
    "2026-09-15"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_REFERENCE_PERIOD: Final = "2012-04"
EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_REFERENCE_PERIOD: Final = "2026-07"
EUROSTAT_INTERNATIONAL_TRADE_GOODS_INTERNAL_GAP_REFERENCE_PERIODS: Final = ()
EUROSTAT_INTERNATIONAL_TRADE_GOODS_PAGE_DATE_OFFSET_EXCEPTIONS: Final[
    dict[str, int]
] = {}
EUROSTAT_INTERNATIONAL_TRADE_GOODS_REFERENCE_PERIOD_INFERENCES: Final[
    dict[str, str]
] = {}
EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT: Final = 5
EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_RESULT_COUNT: Final = 414
EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_UNIQUE_URI_COUNT: Final = 414
EUROSTAT_INTERNATIONAL_TRADE_GOODS_TITLE_MATCH_COUNT: Final = 174
EUROSTAT_INTERNATIONAL_TRADE_GOODS_PREFILTER_COUNT: Final = 172
EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_COUNT: Final = 172
EUROSTAT_INTERNATIONAL_TRADE_GOODS_DOCUMENT_COUNT: Final = 141

EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_URI: Final = (
    "https://ec.europa.eu/eurostat/search"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_URI: Final = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "ext_st_easitc?lang=en&geo=EA21&indic_et=TRD_VAL&"
    "indic_et=TRD_VAL_SCA&partner=EXT_EA21&partner=EA21&"
    "sitc06=TOTAL&stk_flow=EXP&stk_flow=IMP&sinceTimePeriod=2011-01"
)

EUROSTAT_INTERNATIONAL_TRADE_GOODS_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-international-trade-goods-artifact.v1"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_INVENTORY_ENTRY_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-international-trade-goods-inventory-entry.v1"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-international-trade-goods-exclusion.v1"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_INVENTORY_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-international-trade-goods-inventory.v1"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-international-trade-goods-observation.v1"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-international-trade-goods-dataset.v1"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-international-trade-goods-release-value.v1"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-international-trade-goods-published-value.v1"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-international-trade-goods-release.v1"
)
EUROSTAT_INTERNATIONAL_TRADE_GOODS_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-international-trade-goods-archive-manifest.v1"
)

MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_INDEX_PAGES: Final = 32
MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_ENTRIES: Final = 2_048
MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES: Final = 512
MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_OBSERVATIONS: Final = 4_096
MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_ARTIFACTS: Final = 2_048
MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_TOTAL_BYTES: Final = 1_024_000_000
MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_TITLE_CHARS: Final = 512
MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUES_PER_RELEASE: Final = 96

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
_PRODUCT_DOCUMENT_FILENAME_ALIASES: Final[dict[str, tuple[str, ...]]] = {
    # These two official English-document filenames differ from their product
    # codes.  Keep the observed path segment explicit instead of broadening the
    # product-code grammar or treating the summary-only landing as the release.
    "6-16122021-ap": ("6-16122021-+ap-en.pdf",),
    "6-15112023-bp": ("6-15112023-pb-en.pdf",),
}
_MONTH_RE = re.compile(r"\d{4}-(?:0[1-9]|1[0-2])")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")
_SIGNED_NUMBER_RE = re.compile(r"[+-]?\d+(?:\.\d+)?")
_SPACE_RE = re.compile(r"\s+")
_ECONOMIES: Final = ("EA21",)
_ECONOMY_ORDER: Final = {code: index for index, code in enumerate(_ECONOMIES)}
_RELEASE_ECONOMIES: Final = ("EA",)
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
                "Eurostat international trade in goods product URI has no unique product code"
            )
        token = candidates[0]
    else:
        token = parsed.path if parsed.scheme else value
    token = token.casefold()
    path_tokens = token.strip("/").split("/")
    document_alias = next(
        (
            product_code
            for product_code, filenames in _PRODUCT_DOCUMENT_FILENAME_ALIASES.items()
            if any(filename in path_tokens for filename in filenames)
        ),
        None,
    )
    if document_alias is not None:
        return document_alias
    override = next(
        (
            candidate
            for candidate in _PRODUCT_RELEASE_DATE_OVERRIDES
            if candidate in path_tokens
            or re.search(
                rf"(?<![a-z0-9]){re.escape(candidate)}(?=$|[^a-z0-9])",
                token,
            )
            is not None
        ),
        None,
    )
    if override is not None:
        return override
    match = _PRODUCT_CODE_RE.search(token)
    if match is None:
        raise ValueError(
            "Eurostat international trade in goods product code is invalid"
        )
    return match.group(0).casefold()


def _product_release_date(product_code: object) -> str:
    normalized = _product_code(product_code)
    override = _PRODUCT_RELEASE_DATE_OVERRIDES.get(normalized)
    if override is not None:
        return override
    match = _PRODUCT_CODE_RE.fullmatch(normalized)
    if match is None:  # pragma: no cover - guarded by _product_code
        raise ValueError(
            "Eurostat international trade in goods product code is invalid"
        )
    token = f"{int(match.group('year')):04d}-{match.group('month')}-{match.group('day')}"
    return _iso_date(token, "release_date")


def _inventory_prefilter(title: str) -> bool:
    normalized = _required_text(title, "title").casefold()
    return normalized.startswith("euro area international trade in goods ")


def _inventory_selected(title: str, product_code: str) -> bool:
    _product_code(product_code)
    return _inventory_prefilter(title)


def _exclusion_reason(title: str) -> str:
    normalized = _required_text(title, "title").casefold()
    if "current account" in normalized or "balance of payments" in normalized:
        return "balance-of-payments release, not merchandise-trade headline"
    if "job vacancy" in normalized or "labour" in normalized:
        return "labour-market release returned by the broad text search"
    if "trade in services" in normalized:
        return "services-trade release, not merchandise trade in goods"
    if "trade in goods" in normalized:
        return "partner, product, or EU-only trade release outside the monthly euro-area headline"
    return (
        "broad search result outside the monthly euro-area goods-trade headline"
    )


class EurostatInternationalTradeGoodsArtifactRole(str, Enum):
    """Role of one exact official byte artifact."""

    SEARCH_INDEX = "search-index"
    CURRENT_DATASET = "current-dataset"
    RELEASE_HTML = "release-html"
    RELEASE_DOCUMENT_HTML = "release-document-html"
    RELEASE_DOCUMENT_PDF = "release-document-pdf"

    @classmethod
    def from_value(
        cls, value: str | EurostatInternationalTradeGoodsArtifactRole
    ) -> EurostatInternationalTradeGoodsArtifactRole:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat international trade in goods artifact role"
            ) from exc


class EurostatInternationalTradeGoodsMeasure(str, Enum):
    """Flow and adjustment carried by a source-authored trade value."""

    NON_SEASONAL_EXPORTS = "non-seasonally-adjusted-extra-ea-exports"
    NON_SEASONAL_IMPORTS = "non-seasonally-adjusted-extra-ea-imports"
    NON_SEASONAL_BALANCE = "non-seasonally-adjusted-extra-ea-balance"
    NON_SEASONAL_INTRA_TRADE = "non-seasonally-adjusted-intra-ea-trade"
    SEASONAL_EXPORTS = "seasonally-adjusted-extra-ea-exports"
    SEASONAL_IMPORTS = "seasonally-adjusted-extra-ea-imports"
    SEASONAL_BALANCE = "seasonally-adjusted-extra-ea-balance"
    SEASONAL_INTRA_TRADE = "seasonally-adjusted-intra-ea-trade"

    @classmethod
    def from_value(
        cls, value: str | EurostatInternationalTradeGoodsMeasure
    ) -> EurostatInternationalTradeGoodsMeasure:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat international trade in goods measure"
            ) from exc


_CURRENT_DATASET_MEASURES: Final = (
    EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_EXPORTS,
    EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_IMPORTS,
    EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_INTRA_TRADE,
    EurostatInternationalTradeGoodsMeasure.SEASONAL_EXPORTS,
    EurostatInternationalTradeGoodsMeasure.SEASONAL_IMPORTS,
    EurostatInternationalTradeGoodsMeasure.SEASONAL_INTRA_TRADE,
)


@dataclass(frozen=True, slots=True)
class EurostatInternationalTradeGoodsArtifactV1:
    """Content-addressed official international trade in goods evidence artifact."""

    role: EurostatInternationalTradeGoodsArtifactRole
    request_uri: str
    resolved_uri: str
    source_format: OfficialSourceFormat
    content_length: int
    content_sha256: str
    page_number: int | None = None
    product_code: str | None = None
    schema_version: str = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_ARTIFACT_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_ARTIFACT_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat international trade in goods artifact schema"
            )
        role = EurostatInternationalTradeGoodsArtifactRole.from_value(self.role)
        request_uri = _https_uri(self.request_uri, "request_uri")
        resolved_uri = _https_uri(self.resolved_uri, "resolved_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        expected_format = {
            EurostatInternationalTradeGoodsArtifactRole.SEARCH_INDEX: OfficialSourceFormat.ATOM,
            EurostatInternationalTradeGoodsArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
            EurostatInternationalTradeGoodsArtifactRole.RELEASE_HTML: OfficialSourceFormat.HTML,
            EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
            EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
        }[role]
        if source_format is not expected_format:
            raise ValueError(
                "Eurostat international trade in goods artifact role and format differ"
            )
        length = _bounded_int(
            self.content_length,
            "content_length",
            MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_TOTAL_BYTES,
        )
        if length < 1:
            raise ValueError(
                "Eurostat international trade in goods artifact is empty"
            )
        page_number = self.page_number
        product_code = self.product_code
        if role is EurostatInternationalTradeGoodsArtifactRole.SEARCH_INDEX:
            if page_number is None:
                raise ValueError(
                    "Eurostat international trade in goods search artifact needs a page number"
                )
            _bounded_int(
                page_number,
                "page_number",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_INDEX_PAGES,
            )
            if product_code is not None:
                raise ValueError(
                    "Eurostat international trade in goods search artifact has a product code"
                )
        elif (
            role is EurostatInternationalTradeGoodsArtifactRole.CURRENT_DATASET
        ):
            if page_number is not None or product_code is not None:
                raise ValueError(
                    "Eurostat international trade in goods dataset artifact has release metadata"
                )
        else:
            if page_number is not None or product_code is None:
                raise ValueError(
                    "Eurostat international trade in goods release artifact metadata differs"
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
    ) -> EurostatInternationalTradeGoodsArtifactV1:
        return cls(
            role=EurostatInternationalTradeGoodsArtifactRole.from_value(
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
class EurostatInternationalTradeGoodsInventoryEntryV1:
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
    schema_version: str = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_INVENTORY_ENTRY_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_INVENTORY_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat international trade in goods inventory-entry schema"
            )
        product_code = _product_code(self.product_code)
        release_date = _iso_date(self.release_date, "release_date")
        if release_date != _product_release_date(product_code):
            raise ValueError(
                "Eurostat international trade in goods release date differs from product code"
            )
        title = _required_text(
            self.source_title,
            "source_title",
            maximum=MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_TITLE_CHARS,
        )
        if not _inventory_selected(title, product_code):
            raise ValueError(
                "Eurostat international trade in goods inventory entry is outside selection"
            )
        source_uri = _https_uri(self.source_uri, "source_uri")
        parsed_source_uri = urlsplit(source_uri)
        if (
            parsed_source_uri.path != "/eurostat/product"
            or set(parse_qs(parsed_source_uri.query, keep_blank_values=True))
            != {"code"}
            or parsed_source_uri.fragment
        ):
            raise ValueError(
                "Eurostat international trade in goods source URI route differs"
            )
        if _product_code(source_uri) != product_code:
            raise ValueError(
                "Eurostat international trade in goods source URI and product code differ"
            )
        page = _bounded_int(
            self.page_number,
            "page_number",
            MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_INDEX_PAGES,
        )
        if page < 1:
            raise ValueError(
                "Eurostat international trade in goods page number must be positive"
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
            "eurostat-international-trade-goods-inventory-entry",
            self.identity_payload(),
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError(
                "Eurostat international trade in goods inventory-entry identity differs"
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
    ) -> EurostatInternationalTradeGoodsInventoryEntryV1:
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
class EurostatInternationalTradeGoodsIndexExclusionV1:
    """One explicit non-lineage result from the bounded Atom search."""

    product_code: str
    release_date: str
    source_title: str
    source_uri: str
    reason: str
    exclusion_id: str = ""
    schema_version: str = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_EXCLUSION_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_EXCLUSION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat international trade in goods exclusion schema"
            )
        product_code = _product_code(self.product_code)
        release_date = _iso_date(self.release_date, "release_date")
        if release_date != _product_release_date(product_code):
            raise ValueError(
                "Eurostat international trade in goods exclusion date differs"
            )
        title = _required_text(
            self.source_title,
            "source_title",
            maximum=MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_TITLE_CHARS,
        )
        if _inventory_selected(title, product_code):
            raise ValueError(
                "Eurostat international trade in goods exclusion matches selection"
            )
        source_uri = _https_uri(self.source_uri, "source_uri")
        if _product_code(source_uri) != product_code:
            raise ValueError(
                "Eurostat international trade in goods exclusion URI differs"
            )
        reason = _required_text(self.reason, "reason")
        if reason != _exclusion_reason(title):
            raise ValueError(
                "Eurostat international trade in goods exclusion reason differs"
            )
        object.__setattr__(self, "product_code", product_code)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "source_uri", source_uri)
        object.__setattr__(self, "reason", reason)
        expected = _stable_id(
            "eurostat-international-trade-goods-exclusion",
            self.identity_payload(),
        )
        if self.exclusion_id and self.exclusion_id != expected:
            raise ValueError(
                "Eurostat international trade in goods exclusion identity differs"
            )
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
    ) -> EurostatInternationalTradeGoodsIndexExclusionV1:
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
class EurostatInternationalTradeGoodsInventoryV1:
    """Hash-bound official search inventory and deterministic selection."""

    artifacts: tuple[EurostatInternationalTradeGoodsArtifactV1, ...]
    entries: tuple[EurostatInternationalTradeGoodsInventoryEntryV1, ...]
    exclusions: tuple[EurostatInternationalTradeGoodsIndexExclusionV1, ...]
    query_result_count: int
    unique_uri_count: int
    title_match_count: int
    prefilter_count: int
    inventory_id: str = ""
    schema_version: str = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_INVENTORY_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_INVENTORY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat international trade in goods inventory schema"
            )
        artifacts = tuple(self.artifacts)
        entries = tuple(self.entries)
        exclusions = tuple(self.exclusions)
        if (
            len(artifacts)
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT
        ):
            raise ValueError(
                "Eurostat international trade in goods search page count differs"
            )
        if any(
            item.role
            is not EurostatInternationalTradeGoodsArtifactRole.SEARCH_INDEX
            for item in artifacts
        ):
            raise ValueError(
                "Eurostat international trade in goods inventory artifact role differs"
            )
        if [item.page_number for item in artifacts] != list(
            range(1, EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT + 1)
        ):
            raise ValueError(
                "Eurostat international trade in goods search pages are not contiguous"
            )
        if any(
            item.request_uri != _search_uri(cast(int, item.page_number))
            or item.resolved_uri != item.request_uri
            for item in artifacts
        ):
            raise ValueError(
                "Eurostat international trade in goods search artifact identity differs"
            )
        if len(entries) != EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_COUNT:
            raise ValueError(
                "Eurostat international trade in goods selected release count differs"
            )
        codes = [item.product_code for item in entries]
        if len(codes) != len(set(codes)):
            raise ValueError(
                "Eurostat international trade in goods inventory repeats a product code"
            )
        if list(entries) != sorted(
            entries, key=lambda item: (item.release_date, item.product_code)
        ):
            raise ValueError(
                "Eurostat international trade in goods inventory entries are not canonical"
            )
        if (
            entries[0].release_date
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_SEARCH_RELEASE_DATE
        ):
            raise ValueError(
                "Eurostat international trade in goods first searchable release differs"
            )
        if (
            entries[-1].release_date
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_PACKAGED_RELEASE_DATE
        ):
            raise ValueError(
                "Eurostat international trade in goods latest searchable release differs"
            )
        if len(exclusions) != (
            EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_UNIQUE_URI_COUNT
            - EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_COUNT
        ):
            raise ValueError(
                "Eurostat international trade in goods exclusion count differs"
            )
        exclusion_codes = [item.product_code for item in exclusions]
        if len(exclusion_codes) != len(set(exclusion_codes)):
            raise ValueError(
                "Eurostat international trade in goods exclusions repeat a product"
            )
        if list(exclusions) != sorted(
            exclusions, key=lambda item: (item.release_date, item.product_code)
        ):
            raise ValueError(
                "Eurostat international trade in goods exclusions are not canonical"
            )
        if set(codes) & set(exclusion_codes):
            raise ValueError(
                "Eurostat international trade in goods selection includes an exclusion"
            )
        expected_counts = (
            EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_RESULT_COUNT,
            EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_UNIQUE_URI_COUNT,
            EUROSTAT_INTERNATIONAL_TRADE_GOODS_TITLE_MATCH_COUNT,
            EUROSTAT_INTERNATIONAL_TRADE_GOODS_PREFILTER_COUNT,
        )
        observed_counts = (
            _bounded_int(
                self.query_result_count,
                "query_result_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.unique_uri_count,
                "unique_uri_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.title_match_count,
                "title_match_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.prefilter_count,
                "prefilter_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_ENTRIES,
            ),
        )
        if observed_counts != expected_counts:
            raise ValueError(
                "Eurostat international trade in goods search inventory counts differ"
            )
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "exclusions", exclusions)
        expected = _stable_id(
            "eurostat-international-trade-goods-inventory",
            self.identity_payload(),
        )
        if self.inventory_id and self.inventory_id != expected:
            raise ValueError(
                "Eurostat international trade in goods inventory identity differs"
            )
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
    ) -> EurostatInternationalTradeGoodsInventoryV1:
        return cls(
            artifacts=tuple(
                EurostatInternationalTradeGoodsArtifactV1.from_dict(
                    _mapping(item, "artifact")
                )
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            entries=tuple(
                EurostatInternationalTradeGoodsInventoryEntryV1.from_dict(
                    _mapping(item, "inventory entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            exclusions=tuple(
                EurostatInternationalTradeGoodsIndexExclusionV1.from_dict(
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
class EurostatInternationalTradeGoodsObservationV1:
    """One current-revised JSON-stat international-trade-goods observation."""

    economy_code: str
    reference_period: str
    measure: EurostatInternationalTradeGoodsMeasure
    unit: str
    value_lexical: str
    value: float
    status: str | None
    flat_index: int
    observation_id: str = ""
    schema_version: str = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_OBSERVATION_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_OBSERVATION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat international trade in goods observation schema"
            )
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _ECONOMIES:
            raise ValueError(
                "Eurostat international trade in goods observation economy differs"
            )
        reference = _month(self.reference_period, "reference_period")
        measure = EurostatInternationalTradeGoodsMeasure.from_value(
            self.measure
        )
        unit = _required_text(self.unit, "unit")
        if unit != EUROSTAT_INTERNATIONAL_TRADE_GOODS_CURRENT_DATASET_UNIT:
            raise ValueError(
                "Eurostat international trade in goods observation unit differs"
            )
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError(
                "Eurostat international trade in goods observation lexical is invalid"
            )
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError(
                "Eurostat international trade in goods observation numeric differs"
            )
        if not 0.0 <= numeric <= 10_000_000.0:
            raise ValueError(
                "Eurostat international trade in goods observation is outside its bound"
            )
        status = _optional_text(self.status, "status")
        if status is not None and len(status) > 16:
            raise ValueError(
                "Eurostat international trade in goods status exceeds its bound"
            )
        flat_index = _bounded_int(
            self.flat_index,
            "flat_index",
            MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_OBSERVATIONS * 4,
        )
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "flat_index", flat_index)
        expected = _stable_id(
            "eurostat-international-trade-goods-observation",
            self.identity_payload(),
        )
        if self.observation_id and self.observation_id != expected:
            raise ValueError(
                "Eurostat international trade in goods observation identity differs"
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
            "unit": self.unit,
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
    ) -> EurostatInternationalTradeGoodsObservationV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatInternationalTradeGoodsMeasure.from_value(
                str(data.get("measure", ""))
            ),
            unit=str(data.get("unit", "")),
            value_lexical=str(data.get("value_lexical", "")),
            value=float(cast(float, data.get("value"))),
            status=_optional_text(data.get("status"), "status"),
            flat_index=cast(int, data.get("flat_index")),
            observation_id=str(data.get("observation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatInternationalTradeGoodsDatasetV1:
    """Exact current-revised ``ext_st_easitc`` cross-check response."""

    label: str
    updated_at: str
    time_start: str
    time_end: str
    artifact: EurostatInternationalTradeGoodsArtifactV1
    observations: tuple[EurostatInternationalTradeGoodsObservationV1, ...]
    dataset_id: str = "ext_st_easitc"
    dataset_receipt_id: str = ""
    schema_version: str = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat international trade in goods dataset schema"
            )
        if self.dataset_id != "ext_st_easitc":
            raise ValueError(
                "unsupported Eurostat international trade in goods dataset"
            )
        label = _required_text(self.label, "label")
        if label != EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_LABEL:
            raise ValueError(
                "Eurostat international trade in goods dataset label differs"
            )
        updated = _iso_datetime(self.updated_at, "updated_at")
        time_start = _month(self.time_start, "time_start")
        time_end = _month(self.time_end, "time_end")
        if time_end < time_start:
            raise ValueError(
                "Eurostat international trade in goods dataset time range is reversed"
            )
        if (
            self.artifact.role
            is not EurostatInternationalTradeGoodsArtifactRole.CURRENT_DATASET
        ):
            raise ValueError(
                "Eurostat international trade in goods dataset artifact role differs"
            )
        if (
            self.artifact.request_uri
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_URI
            or self.artifact.resolved_uri
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_URI
        ):
            raise ValueError(
                "Eurostat international trade in goods dataset artifact identity differs"
            )
        observations = tuple(self.observations)
        if (
            not 1
            <= len(observations)
            <= MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_OBSERVATIONS
        ):
            raise ValueError(
                "Eurostat international trade in goods observation count is invalid"
            )
        keys = [item.key for item in observations]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "Eurostat international trade in goods dataset repeats an observation"
            )
        periods = _month_range(time_start, time_end)
        expected_keys = {
            (measure.value, economy, period)
            for measure in _CURRENT_DATASET_MEASURES
            for economy in _ECONOMIES
            for period in periods
        }
        if set(keys) != expected_keys:
            raise ValueError(
                "Eurostat international trade in goods dataset cube coverage differs"
            )
        period_positions = {
            period: position for position, period in enumerate(periods)
        }
        measure_coordinates = {
            EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_EXPORTS: (
                1,
                0,
                1,
            ),
            EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_IMPORTS: (
                0,
                0,
                1,
            ),
            EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_INTRA_TRADE: (
                1,
                0,
                0,
            ),
            EurostatInternationalTradeGoodsMeasure.SEASONAL_EXPORTS: (1, 1, 1),
            EurostatInternationalTradeGoodsMeasure.SEASONAL_IMPORTS: (0, 1, 1),
            EurostatInternationalTradeGoodsMeasure.SEASONAL_INTRA_TRADE: (
                1,
                1,
                0,
            ),
        }
        if any(
            item.flat_index
            != (
                measure_coordinates[item.measure][0] * 2 * 2 * len(periods)
                + measure_coordinates[item.measure][1] * 2 * len(periods)
                + measure_coordinates[item.measure][2] * len(periods)
                + period_positions[item.reference_period]
            )
            for item in observations
        ):
            raise ValueError(
                "Eurostat international trade in goods observation coordinate differs"
            )
        if list(observations) != sorted(
            observations,
            key=lambda item: (
                list(EurostatInternationalTradeGoodsMeasure).index(
                    item.measure
                ),
                _ECONOMY_ORDER[item.economy_code],
                item.reference_period,
            ),
        ):
            raise ValueError(
                "Eurostat international trade in goods observations are not canonical"
            )
        if (
            min(item.reference_period for item in observations) != time_start
            or max(item.reference_period for item in observations) != time_end
        ):
            raise ValueError(
                "Eurostat international trade in goods dataset time range differs"
            )
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "updated_at", updated)
        object.__setattr__(self, "time_start", time_start)
        object.__setattr__(self, "time_end", time_end)
        object.__setattr__(self, "observations", observations)
        expected = _stable_id(
            "eurostat-international-trade-goods-dataset",
            self.identity_payload(),
        )
        if self.dataset_receipt_id and self.dataset_receipt_id != expected:
            raise ValueError(
                "Eurostat international trade in goods dataset identity differs"
            )
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
    ) -> EurostatInternationalTradeGoodsDatasetV1:
        return cls(
            label=str(data.get("label", "")),
            updated_at=str(data.get("updated_at", "")),
            time_start=str(data.get("time_start", "")),
            time_end=str(data.get("time_end", "")),
            artifact=EurostatInternationalTradeGoodsArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            observations=tuple(
                EurostatInternationalTradeGoodsObservationV1.from_dict(
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
        MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_INDEX_PAGES,
    )
    if page < 1:
        raise ValueError(
            "Eurostat international trade in goods search page must be positive"
        )
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
        f"{_SEARCH_PREFIX}text": "international trade in goods",
        f"{_SEARCH_PREFIX}sort": "date",
        f"{_SEARCH_PREFIX}collection": "CAT_PREREL",
        f"{_SEARCH_PREFIX}priv_r_p_implicitModel": "true",
        "pageNumber": str(page),
    }
    return (
        f"{EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_URI}?{urlencode(params)}"
    )


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY)
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


def build_eurostat_international_trade_goods_search_requests(
    registry: OfficialSourceRegistryV1,
    *,
    page_count: int = EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the bounded official Atom inventory requests."""
    count = _bounded_int(
        page_count,
        "page_count",
        MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_INDEX_PAGES,
    )
    if count < 1:
        raise ValueError(
            "Eurostat international trade in goods search needs at least one page"
        )
    return tuple(
        _request(
            registry,
            _search_uri(page),
            OfficialSourceFormat.ATOM,
            page_number=page,
        )
        for page in range(1, count + 1)
    )


def build_eurostat_international_trade_goods_dataset_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact current-revised international trade in goods cross-check request."""
    return _request(
        registry,
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_URI,
        OfficialSourceFormat.JSON_STAT,
    )


def build_eurostat_international_trade_goods_release_requests(
    registry: OfficialSourceRegistryV1,
    inventory: EurostatInternationalTradeGoodsInventoryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one primary request for every selected international trade in goods product."""
    if not isinstance(inventory, EurostatInternationalTradeGoodsInventoryV1):
        raise TypeError(
            "Eurostat international trade in goods release requests require a v1 inventory"
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
    raise ValueError(
        "Eurostat international trade in goods release document format differs"
    )


def build_eurostat_international_trade_goods_document_requests(
    registry: OfficialSourceRegistryV1,
    inventory: EurostatInternationalTradeGoodsInventoryV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Discover one English source document for each legacy landing."""
    if not isinstance(inventory, EurostatInternationalTradeGoodsInventoryV1):
        raise TypeError(
            "Eurostat international trade in goods document requests require a v1 inventory"
        )
    entries = {item.product_code: item for item in inventory.entries}
    snapshots = tuple(release_snapshots)
    if len(snapshots) != EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_COUNT:
        raise ValueError(
            "Eurostat international trade in goods landing snapshot count differs"
        )
    by_code: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        code = _product_code(snapshot.request.uri)
        if code in by_code:
            raise ValueError(
                "Eurostat international trade in goods landing corpus repeats a product code"
            )
        by_code[code] = snapshot
    if set(by_code) != set(entries):
        raise ValueError(
            "Eurostat international trade in goods landing corpus is incomplete"
        )
    requests: list[OfficialSourceRequestV1] = []
    for code, entry in entries.items():
        snapshot = by_code[code]
        if snapshot.request.uri != entry.source_uri:
            raise ValueError(
                "Eurostat international trade in goods landing request identity differs"
            )
        _, _, _, document_uris = _parse_release_landing(snapshot, entry)
        requests.extend(
            _request(registry, uri, _document_source_format(uri))
            for uri in document_uris
        )
    if (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_DOCUMENT_COUNT
        and len(requests) != EUROSTAT_INTERNATIONAL_TRADE_GOODS_DOCUMENT_COUNT
    ):
        raise ValueError(
            "Eurostat international trade in goods English document count differs"
        )
    return tuple(requests)


def _artifact_from_snapshot(
    snapshot: OfficialRawSnapshotV1,
    role: EurostatInternationalTradeGoodsArtifactRole,
    *,
    product_code: str | None = None,
) -> EurostatInternationalTradeGoodsArtifactV1:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError(
            "Eurostat international trade in goods artifact requires a v1 snapshot"
        )
    if (
        snapshot.request.source_key
        != EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY
        or snapshot.request.parser_id
        != EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_ID
        or snapshot.request.parser_version
        != EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_VERSION
    ):
        raise ValueError(
            "Eurostat international trade in goods snapshot source or parser differs"
        )
    if snapshot.request.method is not OfficialRequestMethod.GET:
        raise ValueError(
            "Eurostat international trade in goods snapshot method differs"
        )
    expected_format = {
        EurostatInternationalTradeGoodsArtifactRole.SEARCH_INDEX: OfficialSourceFormat.ATOM,
        EurostatInternationalTradeGoodsArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_HTML: OfficialSourceFormat.HTML,
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
    }[role]
    if snapshot.request.source_format is not expected_format:
        raise ValueError(
            "Eurostat international trade in goods snapshot format differs from artifact role"
        )
    if snapshot.status_code != 200:
        raise ValueError(
            "Eurostat international trade in goods snapshot status differs"
        )
    if role is EurostatInternationalTradeGoodsArtifactRole.SEARCH_INDEX:
        if (
            snapshot.request.page_number is None
            or snapshot.request.uri != _search_uri(snapshot.request.page_number)
        ):
            raise ValueError(
                "Eurostat international trade in goods search request identity differs"
            )
        try:
            root = safe_xml_fromstring(snapshot.content)
        except (DefusedXmlException, ParseError) as exc:
            raise ValueError(
                "Eurostat international trade in goods search artifact is invalid XML"
            ) from exc
        if root.tag != f"{{{_ATOM_NAMESPACE}}}feed":
            raise ValueError(
                "Eurostat international trade in goods search artifact is not Atom"
            )
    elif role is EurostatInternationalTradeGoodsArtifactRole.CURRENT_DATASET:
        if (
            snapshot.request.page_number is not None
            or snapshot.request.uri
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_URI
        ):
            raise ValueError(
                "Eurostat international trade in goods dataset request identity differs"
            )
        if not snapshot.content.lstrip().startswith(b"{"):
            raise ValueError(
                "Eurostat international trade in goods dataset artifact is not JSON"
            )
    elif snapshot.request.page_number is not None:
        raise ValueError(
            "Eurostat international trade in goods release request has a page number"
        )
    elif role in {
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_HTML,
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_HTML,
    }:
        if b"<html" not in snapshot.content[:16_384].lower():
            raise ValueError(
                "Eurostat international trade in goods HTML signature differs"
            )
    elif not snapshot.content.startswith(b"%PDF-"):
        raise ValueError(
            "Eurostat international trade in goods PDF signature differs"
        )
    if role in {
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_HTML,
        EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_PDF,
    } and (
        product_code is None
        or _product_code(snapshot.request.uri) != _product_code(product_code)
    ):
        raise ValueError(
            "Eurostat international trade in goods document product code differs"
        )
    return EurostatInternationalTradeGoodsArtifactV1(
        role=role,
        request_uri=snapshot.request.uri,
        resolved_uri=snapshot.resolved_uri,
        source_format=expected_format,
        content_length=len(snapshot.content),
        content_sha256=snapshot.content_sha256,
        page_number=(
            snapshot.request.page_number
            if role is EurostatInternationalTradeGoodsArtifactRole.SEARCH_INDEX
            else None
        ),
        product_code=product_code,
    )


def _atom_text(entry: Element, tag: str) -> str:
    node = entry.find(f"{{{_ATOM_NAMESPACE}}}{tag}")
    if node is None or node.text is None:
        raise ValueError(
            f"Eurostat international trade in goods Atom entry omits {tag}"
        )
    return html.unescape(node.text)


def _atom_link(entry: Element) -> str:
    links = tuple(
        item
        for item in entry.findall(f"{{{_ATOM_NAMESPACE}}}link")
        if item.attrib.get("rel") == "alternate"
    )
    if len(links) != 1 or "href" not in links[0].attrib:
        raise ValueError(
            "Eurostat international trade in goods Atom entry alternate link differs"
        )
    return _https_uri(links[0].attrib["href"], "source_uri")


def parse_eurostat_international_trade_goods_inventory(
    snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatInternationalTradeGoodsInventoryV1:
    """Parse and prove the complete bounded international trade in goods search selection."""
    as_of = _iso_date(as_of_date, "as_of_date")
    values = tuple(snapshots)
    if len(values) != EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT:
        raise ValueError(
            "Eurostat international trade in goods inventory snapshot count differs"
        )
    if [item.request.page_number for item in values] != list(
        range(1, EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT + 1)
    ):
        raise ValueError(
            "Eurostat international trade in goods search snapshots are not contiguous"
        )
    artifacts: list[EurostatInternationalTradeGoodsArtifactV1] = []
    raw_entries: list[tuple[int, str, str, str, str, str]] = []
    for snapshot in values:
        artifact = _artifact_from_snapshot(
            snapshot, EurostatInternationalTradeGoodsArtifactRole.SEARCH_INDEX
        )
        artifacts.append(artifact)
        try:
            root = safe_xml_fromstring(snapshot.content)
        except (DefusedXmlException, ParseError) as exc:
            raise ValueError(
                "Eurostat international trade in goods search page is invalid XML"
            ) from exc
        entries = tuple(root.findall(f"{{{_ATOM_NAMESPACE}}}entry"))
        expected_count = (
            14
            if artifact.page_number
            == EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT
            else 100
        )
        if len(entries) != expected_count:
            raise ValueError(
                "Eurostat international trade in goods search page size differs"
            )
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
    if (
        len(raw_entries)
        != EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_RESULT_COUNT
    ):
        raise ValueError(
            "Eurostat international trade in goods search result count differs"
        )
    unique_by_uri: dict[str, tuple[int, str, str, str, str, str]] = {}
    for item in raw_entries:
        previous = unique_by_uri.get(item[2])
        if previous is not None and previous[1:] != item[1:]:
            raise ValueError(
                "Eurostat international trade in goods search duplicate URI differs"
            )
        unique_by_uri.setdefault(item[2], item)
    if (
        len(unique_by_uri)
        != EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_UNIQUE_URI_COUNT
    ):
        raise ValueError(
            "Eurostat international trade in goods search unique URI count differs"
        )
    unique_entries = tuple(unique_by_uri.values())
    title_match_count = sum(
        re.search(r"\binternational trade in goods\b", title, re.IGNORECASE)
        is not None
        for _, title, _, _, _, _ in unique_entries
    )
    prefiltered = [
        item for item in unique_entries if _inventory_prefilter(item[1])
    ]
    selected: list[EurostatInternationalTradeGoodsInventoryEntryV1] = []
    exclusions: list[EurostatInternationalTradeGoodsIndexExclusionV1] = []
    for page, title, uri, published, updated, summary in unique_entries:
        product_code = _product_code(uri)
        release_date = _product_release_date(product_code)
        if release_date > as_of:
            continue
        if not _inventory_selected(title, product_code):
            exclusions.append(
                EurostatInternationalTradeGoodsIndexExclusionV1(
                    product_code=product_code,
                    release_date=release_date,
                    source_title=title,
                    source_uri=uri,
                    reason=_exclusion_reason(title),
                )
            )
            continue
        selected.append(
            EurostatInternationalTradeGoodsInventoryEntryV1(
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
    return EurostatInternationalTradeGoodsInventoryV1(
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


def parse_eurostat_international_trade_goods_dataset(
    snapshot: OfficialRawSnapshotV1,
) -> EurostatInternationalTradeGoodsDatasetV1:
    """Decode the exact current-revised international trade in goods JSON-stat cross-check."""
    artifact = _artifact_from_snapshot(
        snapshot, EurostatInternationalTradeGoodsArtifactRole.CURRENT_DATASET
    )
    try:
        payload = _mapping(json.loads(snapshot.content), "dataset")
        lexical_payload = _mapping(
            json.loads(snapshot.content, parse_float=str), "lexical dataset"
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "Eurostat international trade in goods dataset is invalid JSON"
        ) from exc
    if (
        payload.get("version") != "2.0"
        or payload.get("class") != "dataset"
        or payload.get("source") != "ESTAT"
        or payload.get("label")
        != EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_LABEL
    ):
        raise ValueError(
            "Eurostat international trade in goods dataset envelope differs"
        )
    extension = _mapping(payload.get("extension"), "extension")
    if (
        extension.get("id") != "EXT_ST_EASITC"
        or extension.get("agencyId") != "ESTAT"
    ):
        raise ValueError(
            "Eurostat international trade in goods dataset identity differs"
        )
    identifiers = [str(item) for item in _sequence(payload.get("id"), "id")]
    expected_ids = [
        "freq",
        "stk_flow",
        "indic_et",
        "partner",
        "sitc06",
        "geo",
        "time",
    ]
    if identifiers != expected_ids:
        raise ValueError(
            "Eurostat international trade in goods dataset dimensions differ"
        )
    sizes = tuple(_sequence(payload.get("size"), "size"))
    if any(
        isinstance(item, bool) or not isinstance(item, int) for item in sizes
    ):
        raise TypeError(
            "Eurostat international trade in goods dimension sizes must be integers"
        )
    dimension = _mapping(payload.get("dimension"), "dimension")
    expected_codes = {
        "freq": ("M",),
        "stk_flow": ("IMP", "EXP"),
        "indic_et": ("TRD_VAL", "TRD_VAL_SCA"),
        "partner": ("EA21", "EXT_EA21"),
        "sitc06": ("TOTAL",),
        "geo": _ECONOMIES,
    }
    observed_codes = {
        name: _dimension_codes(_mapping(dimension[name], name), name)
        for name in expected_codes
    }
    if observed_codes != expected_codes:
        raise ValueError(
            "Eurostat international trade in goods dataset dimensions or codes differ"
        )
    indicator_category = _mapping(
        _mapping(dimension["indic_et"], "indic_et").get("category"),
        "indic_et.category",
    )
    indicator_labels = {
        str(key): str(value)
        for key, value in _mapping(
            indicator_category.get("label"), "indic_et.category.label"
        ).items()
    }
    if indicator_labels != {
        "TRD_VAL": "Trade value in million ECU/EURO",
        "TRD_VAL_SCA": (
            "Seasonally and calendar adjusted trade value in million ECU/EURO"
        ),
    }:
        raise ValueError(
            "Eurostat international trade in goods dataset indicator labels or units differ"
        )
    time_codes = _dimension_codes(_mapping(dimension["time"], "time"), "time")
    if not time_codes or any(
        _MONTH_RE.fullmatch(item) is None for item in time_codes
    ):
        raise ValueError(
            "Eurostat international trade in goods dataset time codes differ"
        )
    if time_codes != _month_range(time_codes[0], time_codes[-1]):
        raise ValueError(
            "Eurostat international trade in goods dataset time codes are not contiguous"
        )
    expected_sizes = (1, 2, 2, 2, 1, len(_ECONOMIES), len(time_codes))
    if tuple(cast(Sequence[int], sizes)) != expected_sizes:
        raise ValueError(
            "Eurostat international trade in goods dataset sizes differ"
        )
    values = _mapping(payload.get("value"), "value")
    lexical_values = _mapping(lexical_payload.get("value"), "lexical value")
    statuses = _mapping(payload.get("status", {}), "status")
    if set(values) != set(lexical_values):
        raise ValueError(
            "Eurostat international trade in goods lexical and numeric values differ"
        )
    if not set(statuses) <= set(values):
        raise ValueError(
            "Eurostat international trade in goods status has no observation"
        )
    observations: list[EurostatInternationalTradeGoodsObservationV1] = []
    time_count = len(time_codes)
    maximum = 2 * 2 * 2 * len(_ECONOMIES) * time_count
    for raw_index, raw_value in values.items():
        try:
            flat_index = int(str(raw_index))
        except ValueError as exc:
            raise ValueError(
                "Eurostat international trade in goods sparse index is invalid"
            ) from exc
        if not 0 <= flat_index < maximum:
            raise ValueError(
                "Eurostat international trade in goods sparse index is outside its cube"
            )
        flow_index, remainder = divmod(flat_index, 2 * 2 * time_count)
        indicator_index, remainder = divmod(remainder, 2 * time_count)
        partner_index, time_index = divmod(remainder, time_count)
        measure = {
            (
                1,
                0,
                1,
            ): EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_EXPORTS,
            (
                0,
                0,
                1,
            ): EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_IMPORTS,
            (
                1,
                0,
                0,
            ): EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_INTRA_TRADE,
            (1, 1, 1): EurostatInternationalTradeGoodsMeasure.SEASONAL_EXPORTS,
            (0, 1, 1): EurostatInternationalTradeGoodsMeasure.SEASONAL_IMPORTS,
            (
                1,
                1,
                0,
            ): EurostatInternationalTradeGoodsMeasure.SEASONAL_INTRA_TRADE,
        }.get((flow_index, indicator_index, partner_index))
        if measure is None:
            # Intra-area imports are present in the retained cube but are not a
            # value published by this release family; do not relabel them as
            # the source-authored intra-area trade (exports) series.
            continue
        lexical = str(lexical_values[raw_index])
        observations.append(
            EurostatInternationalTradeGoodsObservationV1(
                economy_code="EA21",
                reference_period=time_codes[time_index],
                measure=measure,
                unit=EUROSTAT_INTERNATIONAL_TRADE_GOODS_CURRENT_DATASET_UNIT,
                value_lexical=lexical,
                value=float(cast(float, raw_value)),
                status=cast(str | None, statuses.get(raw_index)),
                flat_index=flat_index,
            )
        )
    observations.sort(
        key=lambda item: (
            list(EurostatInternationalTradeGoodsMeasure).index(item.measure),
            _ECONOMY_ORDER[item.economy_code],
            item.reference_period,
        )
    )
    return EurostatInternationalTradeGoodsDatasetV1(
        label=str(payload.get("label", "")),
        updated_at=str(payload.get("updated", "")),
        time_start=time_codes[0],
        time_end=time_codes[-1],
        artifact=artifact,
        observations=tuple(observations),
    )


class EurostatInternationalTradeGoodsMovement(str, Enum):
    """Balance wording attached to the source-authored headline value."""

    SURPLUS = "surplus"
    DEFICIT = "deficit"

    @classmethod
    def from_value(
        cls, value: str | EurostatInternationalTradeGoodsMovement
    ) -> EurostatInternationalTradeGoodsMovement:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat international trade in goods movement"
            ) from exc


def _headline_measure(
    reference_period: str,
) -> EurostatInternationalTradeGoodsMeasure:
    _month(reference_period, "reference_period")
    return EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_BALANCE


def _headline_period_scope(reference_period: str) -> str:
    reference = _month(reference_period, "reference_period")
    return "year-to-date" if reference.endswith("-12") else "month"


@dataclass(frozen=True, slots=True)
class EurostatInternationalTradeGoodsReleaseValueV1:
    """Source-era euro-area goods-trade balance in a release headline."""

    economy_code: str
    reference_period: str
    measure: EurostatInternationalTradeGoodsMeasure
    unit: str
    period_scope: str
    movement: EurostatInternationalTradeGoodsMovement
    value_lexical: str
    value: float
    evidence_id: str
    release_value_id: str = ""
    schema_version: str = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_VALUE_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_VALUE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat international trade in goods release-value schema"
            )
        if self.economy_code != "EA":
            raise ValueError(
                "Eurostat international trade in goods release value must retain source-era EA scope"
            )
        reference = _month(self.reference_period, "reference_period")
        measure = EurostatInternationalTradeGoodsMeasure.from_value(
            self.measure
        )
        if measure is not _headline_measure(reference):
            raise ValueError(
                "Eurostat international trade in goods headline measure differs"
            )
        unit = _required_text(self.unit, "unit")
        if unit != EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_UNIT:
            raise ValueError(
                "Eurostat international trade in goods headline unit differs"
            )
        period_scope = _required_text(self.period_scope, "period_scope")
        if period_scope != _headline_period_scope(reference):
            raise ValueError(
                "Eurostat international trade in goods headline scope differs"
            )
        movement = EurostatInternationalTradeGoodsMovement.from_value(
            self.movement
        )
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None or lexical.startswith("-"):
            raise ValueError(
                "Eurostat international trade in goods headline value is invalid"
            )
        magnitude = float(lexical)
        numeric = {
            EurostatInternationalTradeGoodsMovement.DEFICIT: -magnitude,
        }.get(movement, magnitude)
        if self.value != numeric or not math.isfinite(self.value):
            raise ValueError(
                "Eurostat international trade in goods headline numeric value differs"
            )
        if not -1_000.0 <= self.value <= 1_000.0:
            raise ValueError(
                "Eurostat international trade in goods headline value is outside its bound"
            )
        evidence_id = _required_text(self.evidence_id, "evidence_id")
        if not evidence_id.startswith(
            "eurostat-international-trade-goods-inventory-entry:sha256:"
        ):
            raise ValueError(
                "Eurostat international trade in goods headline evidence differs"
            )
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "period_scope", period_scope)
        object.__setattr__(self, "movement", movement)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "evidence_id", evidence_id)
        expected = _stable_id(
            "eurostat-international-trade-goods-release-value",
            self.identity_payload(),
        )
        if self.release_value_id and self.release_value_id != expected:
            raise ValueError(
                "Eurostat international trade in goods release-value identity differs"
            )
        object.__setattr__(self, "release_value_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "economy_code": self.economy_code,
            "reference_period": self.reference_period,
            "measure": self.measure.value,
            "unit": self.unit,
            "period_scope": self.period_scope,
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
    ) -> EurostatInternationalTradeGoodsReleaseValueV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatInternationalTradeGoodsMeasure.from_value(
                str(data.get("measure", ""))
            ),
            unit=str(data.get("unit", "")),
            period_scope=str(data.get("period_scope", "")),
            movement=EurostatInternationalTradeGoodsMovement.from_value(
                str(data.get("movement", ""))
            ),
            value_lexical=str(data.get("value_lexical", "")),
            value=float(cast(float, data.get("value"))),
            evidence_id=str(data.get("evidence_id", "")),
            release_value_id=str(data.get("release_value_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatInternationalTradeGoodsPublishedValueV1:
    """One source-era value read from a release publication table."""

    economy_code: str
    reference_period: str
    measure: EurostatInternationalTradeGoodsMeasure
    unit: str
    value_lexical: str
    value: float
    evidence_artifact_sha256: str
    evidence_locator: str
    published_value_id: str = ""
    schema_version: str = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUE_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat international trade in goods published-value schema"
            )
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _RELEASE_ECONOMIES:
            raise ValueError(
                "Eurostat international trade in goods published-value economy differs"
            )
        reference = _month(self.reference_period, "reference_period")
        measure = EurostatInternationalTradeGoodsMeasure.from_value(
            self.measure
        )
        unit = _required_text(self.unit, "unit")
        if unit != EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_UNIT:
            raise ValueError(
                "Eurostat international trade in goods published-value unit differs"
            )
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _SIGNED_NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError(
                "Eurostat international trade in goods published-value lexical is invalid"
            )
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError(
                "Eurostat international trade in goods published-value numeric differs"
            )
        if not -1_000.0 <= numeric <= 1_000.0:
            raise ValueError(
                "Eurostat international trade in goods published value is outside its bound"
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
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "evidence_artifact_sha256", digest)
        object.__setattr__(self, "evidence_locator", locator)
        expected = _stable_id(
            "eurostat-international-trade-goods-published-value",
            self.identity_payload(),
        )
        if self.published_value_id and self.published_value_id != expected:
            raise ValueError(
                "Eurostat international trade in goods published-value identity differs"
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
            "unit": self.unit,
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
    ) -> EurostatInternationalTradeGoodsPublishedValueV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatInternationalTradeGoodsMeasure.from_value(
                str(data.get("measure", ""))
            ),
            unit=str(data.get("unit", "")),
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
        ("2000-12", "EA11"),
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
    if not 20 <= lag <= 75:
        raise ValueError(
            "Eurostat international trade in goods release lag is outside the qualified window"
        )
    return lag


_HEADLINE_VALUE_RE = re.compile(
    r"\binternational trade in goods\s+"
    r"(?P<movement>surplus|deficit)(?:\s+of)?\s+"
    r"(?:€\s*)?(?P<value>\d+(?:\.\d+)?)\s*bn(?:\s+euro)?\b",
    re.IGNORECASE,
)


def _headline_value(
    entry: EurostatInternationalTradeGoodsInventoryEntryV1,
    reference_period: str,
) -> EurostatInternationalTradeGoodsReleaseValueV1:
    match = _HEADLINE_VALUE_RE.search(entry.source_title)
    if match is None:
        raise ValueError(
            "Eurostat international trade in goods release headline has no balance"
        )
    movement = EurostatInternationalTradeGoodsMovement.from_value(
        match.group("movement").casefold()
    )
    lexical = match.group("value")
    magnitude = float(lexical)
    numeric = {
        EurostatInternationalTradeGoodsMovement.DEFICIT: -magnitude,
    }.get(movement, magnitude)
    return EurostatInternationalTradeGoodsReleaseValueV1(
        economy_code="EA",
        reference_period=reference_period,
        measure=_headline_measure(reference_period),
        unit=EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_UNIT,
        period_scope=_headline_period_scope(reference_period),
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
    entry: EurostatInternationalTradeGoodsInventoryEntryV1,
) -> tuple[str, str, str | None, tuple[str, ...]]:
    try:
        content = snapshot.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "Eurostat international trade in goods release landing is not UTF-8"
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
        raise ValueError(
            "Eurostat international trade in goods Atom and landing titles differ"
        )
    text = " ".join(parser.text)
    page_dates: set[str] = set()
    for date_match in _LANDING_DATE_RE.finditer(text):
        month = _MONTHS.get(date_match.group("month").casefold())
        if month is None:
            raise ValueError(
                "Eurostat international trade in goods landing month is unsupported"
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
                "Eurostat international trade in goods landing date is invalid"
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
            "Eurostat international trade in goods release landing has no unique publication time"
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
            "Eurostat international trade in goods release landing has no unique release date"
        )
    page_date = next(iter(page_dates))
    document_uris_by_identity: dict[
        tuple[str, str, str, tuple[tuple[str, str], ...]], set[str]
    ] = {}
    for href in parser.hrefs:
        uri = _https_uri(urljoin(snapshot.resolved_uri, href), "document_uri")
        try:
            linked_code = _product_code(uri)
        except ValueError:
            continue
        parsed_uri = urlsplit(uri)
        path = parsed_uri.path.casefold()
        path_tokens = path.strip("/").split("/")
        english_document = f"{entry.product_code}-en." in path or any(
            filename in path_tokens
            for filename in _PRODUCT_DOCUMENT_FILENAME_ALIASES.get(
                entry.product_code, ()
            )
        )
        if linked_code == entry.product_code and english_document:
            identity = (
                parsed_uri.scheme,
                parsed_uri.netloc,
                parsed_uri.path,
                tuple(
                    sorted(
                        (key, value)
                        for key, value in parse_qsl(
                            parsed_uri.query, keep_blank_values=True
                        )
                        if key.casefold() != "download"
                    )
                ),
            )
            document_uris_by_identity.setdefault(identity, set()).add(uri)
    if len(document_uris_by_identity) > 1:
        raise ValueError(
            "Eurostat international trade in goods landing exposes multiple English release documents"
        )
    document_uris = tuple(
        min(
            variants,
            key=lambda uri: (
                any(
                    key.casefold() == "download"
                    for key, _ in parse_qsl(
                        urlsplit(uri).query, keep_blank_values=True
                    )
                ),
                len(uri),
                uri,
            ),
        )
        for variants in document_uris_by_identity.values()
    )
    return titles[0], page_date, published_at, document_uris


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


@lru_cache(maxsize=MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES)
def _trade_pdf_page_texts(content: bytes) -> tuple[str, ...]:
    """Extract each retained PDF once per process for all replay consumers."""
    try:
        reader = PdfReader(BytesIO(content), strict=False)
        page_text: list[str] = []
        failed_pages: list[int] = []
        for page_number, page in enumerate(reader.pages, start=1):
            try:
                page_text.append(page.extract_text() or "")
            except PdfReadError:
                failed_pages.append(page_number)
                page_text.append("")
        if len(failed_pages) > 1 or not any(item.strip() for item in page_text):
            raise ValueError(
                "Eurostat international trade in goods release PDF has too many unreadable pages"
            )
        return tuple(page_text)
    except PdfReadError as exc:
        raise ValueError(
            "Eurostat international trade in goods release PDF text extraction failed"
        ) from exc


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
        text = " ".join(_trade_pdf_page_texts(snapshot.content))
    else:
        raise ValueError(
            "Eurostat international trade in goods release value source format differs"
        )
    normalized = _SPACE_RE.sub(" ", text.replace("\u2212", "-")).strip()
    if not normalized:
        raise ValueError(
            "Eurostat international trade in goods release value source has no text"
        )
    return normalized


_REFERENCE_MONTH_RE = re.compile(
    r"\b(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(?P<year>20\d{2})\b",
    re.IGNORECASE,
)
_EA_COMPOSITION_NUMBER_PATTERN: Final = r"(?:11|12|13|15|16|17|18|19|20|21)"
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
    entry: EurostatInternationalTradeGoodsInventoryEntryV1,
    landing_snapshot: OfficialRawSnapshotV1,
    document_snapshot: OfficialRawSnapshotV1 | None,
) -> tuple[str, int]:
    snapshots = (
        (document_snapshot, landing_snapshot)
        if document_snapshot is not None
        else (landing_snapshot,)
    )
    source_texts = chain(
        (_release_source_text(snapshot) for snapshot in snapshots),
        (entry.summary,),
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
    inferred = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_REFERENCE_PERIOD_INFERENCES.get(
            entry.product_code
        )
    )
    if inferred is not None:
        return inferred, _release_lag(entry.release_date, inferred)
    raise ValueError(
        "Eurostat international trade in goods publication omits a qualified reference month for "
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
            "Eurostat international trade in goods source-stated EA composition differs: "
            f"expected={expected}, observed={tuple(sorted(labels))}"
        )
    return expected, expected in labels


# Goods-trade releases have a strong invariant: the first euro-area
# flow table is non-seasonally adjusted and the annex's first flow table is
# seasonally adjusted.  Parse those named rows directly so EU/product/partner
# tables can never be mistaken for the headline series.
_TRADE_ROW_NUMBER_RE = re.compile(
    r"(?<![A-Za-z0-9])(?P<value>[+-]?\d+(?:\.\d+)?)(?P<percent>\s*%)?"
)
_TRADE_ROW_LABELS: Final = {
    "exports": r"Extra[- ]EA(?:\d+)?\s+exports",
    "imports": r"Extra[- ]EA(?:\d+)?\s+imports",
    "balance": r"(?:Extra[- ]EA(?:\d+)?\s+trade balance|Balance)",
    "intra": r"Intra[- ]EA(?:\d+)?(?:\s+(?:trade|dispatches)(?:\d+)?)?",
}
_UNAVAILABLE_TRADE_ROWS: Final[dict[str, frozenset[tuple[bool, str]]]] = {
    # The February 2014 publication explicitly omits the complete seasonal
    # annex because of technical problems.
    "6-14022014-bp": frozenset(
        (True, row) for row in ("exports", "imports", "balance", "intra")
    ),
    # The March 2014 seasonal table publishes colons, not values, for the
    # intra-area row while retaining the other three EA flow rows.
    "6-18032014-ap": frozenset({(True, "intra")}),
    # The March 2015 publication explicitly says seasonal data were not
    # available in time and contains no seasonal annex.
    "6-18032015-ap": frozenset(
        (True, row) for row in ("exports", "imports", "balance", "intra")
    ),
}
_UNAVAILABLE_TRADE_ROW_EVIDENCE: Final[dict[str, re.Pattern[str]]] = {
    "6-14022014-bp": re.compile(
        r"seasonally adjusted data are not available", re.IGNORECASE
    ),
    "6-18032014-ap": re.compile(
        r"seasonally adjusted data for January 2014 for the EA18 are not available",
        re.IGNORECASE,
    ),
    "6-18032015-ap": re.compile(
        r"seasonally adjusted data were not available in time",
        re.IGNORECASE,
    ),
}


def _normalized_trade_text(snapshot: OfficialRawSnapshotV1) -> str:
    text = _release_source_text(snapshot)
    text = re.sub(r"(?<=\.)\s+(?=\d)", "", text)
    return _SPACE_RE.sub(" ", text.replace("\u20ac", " euro ")).strip()


def _trade_section(text: str, *, seasonal: bool) -> str:
    if seasonal:
        annex = re.search(
            r"\bAnnex\s*-\s*Seasonally adjusted data\b", text, re.IGNORECASE
        )
        if annex is None:
            raise ValueError("goods-trade publication omits its seasonal annex")
        candidate = text[annex.start() :]
        table = re.search(
            r"\b(?:EA(?:\d+)?|Euro area|EA and EU)\s+trade\s*-\s*"
            r"seasonally adjusted data\b",
            candidate,
            re.IGNORECASE,
        )
    else:
        candidate = text
        table = re.search(
            r"\b(?:EA(?:\d+)?|Euro area)\s+trade\s*-\s*"
            r"non seasonally adjusted data\b",
            candidate,
            re.IGNORECASE,
        )
    if table is None:
        qualifier = "seasonal" if seasonal else "non-seasonal"
        raise ValueError(
            f"goods-trade publication omits its {qualifier} EA table"
        )
    section = candidate[table.start() :]
    if re.search(r"\bbn\s+euro\b", section[:300], re.IGNORECASE) is None:
        raise ValueError("goods-trade publication table unit differs")
    return section


def _trade_row_lexicals(section: str, row_name: str) -> tuple[str, ...]:
    label = _TRADE_ROW_LABELS[row_name]
    row = re.search(
        rf"\b{label}\b(?P<body>.*?)(?="
        rf"\b(?:{_TRADE_ROW_LABELS['exports']}|"
        rf"{_TRADE_ROW_LABELS['imports']}|"
        rf"{_TRADE_ROW_LABELS['balance']}|"
        rf"{_TRADE_ROW_LABELS['intra']})\b|"
        r"\b(?:EU\d+|European Union|EU trade)\b|"
        r"\bIn (?:January|February|March|April|May|June|July|August|"
        r"September|October|November|December) 20\d{2}\b|"
        r"\b(?:The )?Source dataset\b|$)",
        section,
        re.IGNORECASE,
    )
    if row is None:
        raise ValueError(f"goods-trade publication omits EA {row_name} row")
    return tuple(
        match.group("value")
        for match in _TRADE_ROW_NUMBER_RE.finditer(row.group("body"))
        if match.group("percent") is None
    )


def _trade_source_published_values(
    snapshot: OfficialRawSnapshotV1,
    artifact: EurostatInternationalTradeGoodsArtifactV1,
    reference_period: str,
) -> tuple[EurostatInternationalTradeGoodsPublishedValueV1, ...]:
    text = _normalized_trade_text(snapshot)
    product_code = cast(str, artifact.product_code)
    unavailable_rows = _UNAVAILABLE_TRADE_ROWS.get(product_code, frozenset())
    if unavailable_rows:
        evidence = _UNAVAILABLE_TRADE_ROW_EVIDENCE[product_code]
        if evidence.search(text) is None:
            raise ValueError(
                "goods-trade unavailable-row exception lacks source evidence"
            )
    prior_year = _month_shift(reference_period, -12)
    prior_month = _month_shift(reference_period, -1)
    result: list[EurostatInternationalTradeGoodsPublishedValueV1] = []
    specifications = (
        (
            False,
            "exports",
            EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_EXPORTS,
        ),
        (
            False,
            "imports",
            EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_IMPORTS,
        ),
        (
            False,
            "balance",
            EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_BALANCE,
        ),
        (
            False,
            "intra",
            EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_INTRA_TRADE,
        ),
        (
            True,
            "exports",
            EurostatInternationalTradeGoodsMeasure.SEASONAL_EXPORTS,
        ),
        (
            True,
            "imports",
            EurostatInternationalTradeGoodsMeasure.SEASONAL_IMPORTS,
        ),
        (
            True,
            "balance",
            EurostatInternationalTradeGoodsMeasure.SEASONAL_BALANCE,
        ),
        (
            True,
            "intra",
            EurostatInternationalTradeGoodsMeasure.SEASONAL_INTRA_TRADE,
        ),
    )
    sections = {
        seasonal: _trade_section(text, seasonal=seasonal)
        for seasonal in (False, True)
        if any(
            (seasonal, row_name) not in unavailable_rows
            for row_name in ("exports", "imports", "balance", "intra")
        )
    }
    modern_seasonal = (
        True in sections
        and re.search(
            r"previous three months|[A-Z][a-z]{2}-[A-Z][a-z]{2}\s+\d{2,4}",
            sections[True][:2_000],
            re.IGNORECASE,
        )
        is not None
    )
    for seasonal, row_name, measure in specifications:
        if (seasonal, row_name) in unavailable_rows:
            continue
        lexicals = _trade_row_lexicals(sections[seasonal], row_name)
        if len(lexicals) < 2:
            raise ValueError(
                f"goods-trade EA {row_name} row has fewer than two values"
            )
        selected = (
            lexicals[:2] if not seasonal or modern_seasonal else lexicals[-2:]
        )
        periods = (
            (prior_month, reference_period)
            if seasonal
            else (
                prior_year,
                reference_period,
            )
        )
        for position, (period, lexical) in enumerate(
            zip(periods, selected, strict=True)
        ):
            result.append(
                EurostatInternationalTradeGoodsPublishedValueV1(
                    economy_code="EA",
                    reference_period=period,
                    measure=measure,
                    unit=EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_UNIT,
                    value_lexical=lexical.removeprefix("+"),
                    value=float(lexical),
                    evidence_artifact_sha256=artifact.content_sha256,
                    evidence_locator=(
                        f"source EA {'seasonally adjusted' if seasonal else 'non-seasonally adjusted'} "
                        f"{row_name} row; {'comparison' if position == 0 else 'reference'} month"
                    ),
                )
            )
    result.sort(
        key=lambda item: (
            item.reference_period,
            _RELEASE_ECONOMY_ORDER[item.economy_code],
            list(EurostatInternationalTradeGoodsMeasure).index(item.measure),
        )
    )
    return tuple(result)


def _published_values(
    landing_snapshot: OfficialRawSnapshotV1,
    landing_artifact: EurostatInternationalTradeGoodsArtifactV1,
    document_snapshot: OfficialRawSnapshotV1 | None,
    document_artifact: EurostatInternationalTradeGoodsArtifactV1 | None,
    reference_period: str,
    headline: EurostatInternationalTradeGoodsReleaseValueV1,
) -> tuple[EurostatInternationalTradeGoodsPublishedValueV1, ...]:
    if document_snapshot is not None and document_artifact is not None:
        snapshot = document_snapshot
        artifact = document_artifact
    else:
        snapshot = landing_snapshot
        artifact = landing_artifact
    values = _trade_source_published_values(
        snapshot,
        artifact,
        reference_period,
    )
    if headline.period_scope == "month":
        table_balance = next(
            item
            for item in values
            if item.reference_period == reference_period
            and item.measure
            is EurostatInternationalTradeGoodsMeasure.NON_SEASONAL_BALANCE
        )
        if table_balance.value != headline.value:
            raise ValueError(
                "Eurostat goods-trade headline and monthly table balance differ for "
                f"{landing_artifact.product_code}: headline={headline.value}, "
                f"table={table_balance.value}"
            )
    return values


@dataclass(frozen=True, slots=True)
class EurostatInternationalTradeGoodsReleaseV1:
    """One source-dated monthly international trade in goods release."""

    inventory_entry: EurostatInternationalTradeGoodsInventoryEntryV1
    reference_period: str
    reference_period_source_stated: bool
    days_after_period_end: int
    euro_area_composition: str
    composition_source_stated: bool
    page_release_date: str
    page_release_date_offset_days: int
    published_at: str | None
    linked_document_uris: tuple[str, ...]
    artifact: EurostatInternationalTradeGoodsArtifactV1
    document_artifact: EurostatInternationalTradeGoodsArtifactV1 | None
    headline_value: EurostatInternationalTradeGoodsReleaseValueV1
    published_values: tuple[
        EurostatInternationalTradeGoodsPublishedValueV1, ...
    ]
    release_id: str = ""
    schema_version: str = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat international trade in goods release schema"
            )
        reference = _month(self.reference_period, "reference_period")
        if not isinstance(self.reference_period_source_stated, bool):
            raise TypeError("reference_period_source_stated must be boolean")
        inferred_reference = (
            EUROSTAT_INTERNATIONAL_TRADE_GOODS_REFERENCE_PERIOD_INFERENCES.get(
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
                "Eurostat international trade in goods reference-period evidence differs"
            )
        expected_lag = _release_lag(
            self.inventory_entry.release_date, reference
        )
        if self.days_after_period_end != expected_lag:
            raise ValueError(
                "Eurostat international trade in goods release period or lag differs"
            )
        composition = _required_text(
            self.euro_area_composition, "euro_area_composition"
        )
        if composition != _ea_composition_for_period(reference):
            raise ValueError(
                "Eurostat international trade in goods EA composition differs"
            )
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
            EUROSTAT_INTERNATIONAL_TRADE_GOODS_PAGE_DATE_OFFSET_EXCEPTIONS.get(
                self.inventory_entry.product_code
            )
        )
        if self.page_release_date_offset_days != offset or (
            (expected_exception is None and offset not in {-1, 0})
            or (expected_exception is not None and offset != expected_exception)
        ):
            raise ValueError(
                "Eurostat international trade in goods landing date offset differs for "
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
                    "Eurostat international trade in goods publication timestamp date differs"
                )
        links = tuple(
            _https_uri(item, "linked_document_uri")
            for item in self.linked_document_uris
        )
        if links != tuple(sorted(set(links))):
            raise ValueError(
                "Eurostat international trade in goods release document links are not canonical"
            )
        if len(links) > 1:
            raise ValueError(
                "Eurostat international trade in goods release has multiple English documents"
            )
        if any(
            _product_code(item) != self.inventory_entry.product_code
            for item in links
        ):
            raise ValueError(
                "Eurostat international trade in goods release document product code differs"
            )
        if (
            self.artifact.role
            is not EurostatInternationalTradeGoodsArtifactRole.RELEASE_HTML
        ):
            raise ValueError(
                "Eurostat international trade in goods release artifact role differs"
            )
        if (
            self.artifact.product_code != self.inventory_entry.product_code
            or self.artifact.request_uri != self.inventory_entry.source_uri
            or _product_code(self.artifact.resolved_uri)
            != self.inventory_entry.product_code
        ):
            raise ValueError(
                "Eurostat international trade in goods release artifact product code differs"
            )
        document = self.document_artifact
        if (document is None) != (not links):
            raise ValueError(
                "Eurostat international trade in goods release document artifact differs"
            )
        if document is not None and (
            document.role
            not in {
                EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_HTML,
                EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_PDF,
            }
            or document.request_uri != links[0]
            or document.product_code != self.inventory_entry.product_code
            or _product_code(document.resolved_uri)
            != self.inventory_entry.product_code
        ):
            raise ValueError(
                "Eurostat international trade in goods release document identity differs"
            )
        value = self.headline_value
        if (
            value.reference_period != reference
            or value.measure is not _headline_measure(reference)
            or value.period_scope != _headline_period_scope(reference)
            or value.evidence_id != self.inventory_entry.entry_id
        ):
            raise ValueError(
                "Eurostat international trade in goods release headline occurrence differs"
            )
        published_values = tuple(self.published_values)
        if not (
            1
            <= len(published_values)
            <= MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUES_PER_RELEASE
        ):
            raise ValueError(
                "Eurostat international trade in goods published-value count differs"
            )
        keys = [item.key for item in published_values]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "Eurostat international trade in goods release repeats a published value"
            )
        if list(published_values) != sorted(
            published_values,
            key=lambda item: (
                item.reference_period,
                _RELEASE_ECONOMY_ORDER[item.economy_code],
                list(EurostatInternationalTradeGoodsMeasure).index(
                    item.measure
                ),
            ),
        ):
            raise ValueError(
                "Eurostat international trade in goods published values are not canonical"
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
                "Eurostat international trade in goods published-value evidence differs"
            )
        headline_published = next(
            (
                item
                for item in published_values
                if item.economy_code == "EA"
                and item.measure is value.measure
                and item.reference_period == reference
            ),
            None,
        )
        if value.period_scope == "month" and (
            headline_published is None
            or headline_published.value != value.value
        ):
            raise ValueError(
                "Eurostat international trade in goods headline published value differs"
            )
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "euro_area_composition", composition)
        object.__setattr__(self, "page_release_date", page_date)
        object.__setattr__(self, "published_at", published_at)
        object.__setattr__(self, "linked_document_uris", links)
        object.__setattr__(self, "published_values", published_values)
        expected = _stable_id(
            "eurostat-international-trade-goods-release",
            self.identity_payload(),
        )
        if self.release_id and self.release_id != expected:
            raise ValueError(
                "Eurostat international trade in goods release identity differs"
            )
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
    ) -> EurostatInternationalTradeGoodsReleaseV1:
        return cls(
            inventory_entry=EurostatInternationalTradeGoodsInventoryEntryV1.from_dict(
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
            artifact=EurostatInternationalTradeGoodsArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            document_artifact=(
                EurostatInternationalTradeGoodsArtifactV1.from_dict(
                    _mapping(data.get("document_artifact"), "document_artifact")
                )
                if data.get("document_artifact") is not None
                else None
            ),
            headline_value=EurostatInternationalTradeGoodsReleaseValueV1.from_dict(
                _mapping(data.get("headline_value"), "headline_value")
            ),
            published_values=tuple(
                EurostatInternationalTradeGoodsPublishedValueV1.from_dict(
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
    releases: Sequence[EurostatInternationalTradeGoodsReleaseV1],
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
    releases: Sequence[EurostatInternationalTradeGoodsReleaseV1],
    dataset: EurostatInternationalTradeGoodsDatasetV1,
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
            # Current dissemination values are million euro while releases
            # publish one-decimal billion-euro values.
            changed_count += round(revised / 1_000.0, 1) != value.value
    return comparison_count, changed_count


@dataclass(frozen=True, slots=True)
class EurostatInternationalTradeGoodsArchiveManifestV1:
    """Compact, replayable receipt for the official international trade in goods corpus."""

    as_of_date: str
    registry_id: str
    source_id: str
    inventory: EurostatInternationalTradeGoodsInventoryV1
    current_dataset: EurostatInternationalTradeGoodsDatasetV1
    releases: tuple[EurostatInternationalTradeGoodsReleaseV1, ...]
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
    schema_version: str = (
        EUROSTAT_INTERNATIONAL_TRADE_GOODS_MANIFEST_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat international trade in goods manifest schema"
            )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        if as_of != EUROSTAT_INTERNATIONAL_TRADE_GOODS_AS_OF_DATE:
            raise ValueError(
                "Eurostat international trade in goods archive boundary differs"
            )
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError(
                "Eurostat international trade in goods registry identity is invalid"
            )
        if not source_id.startswith("official-source:sha256:"):
            raise ValueError(
                "Eurostat international trade in goods source identity is invalid"
            )
        releases = tuple(self.releases)
        if len(releases) != EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_COUNT:
            raise ValueError(
                "Eurostat international trade in goods manifest release count differs"
            )
        if list(releases) != sorted(
            releases,
            key=lambda item: (
                item.inventory_entry.release_date,
                item.inventory_entry.product_code,
            ),
        ):
            raise ValueError(
                "Eurostat international trade in goods releases are not canonical"
            )
        if [item.inventory_entry for item in releases] != list(
            self.inventory.entries
        ):
            raise ValueError(
                "Eurostat international trade in goods release and inventory entries differ"
            )
        if any(
            item.headline_value
            != _headline_value(item.inventory_entry, item.reference_period)
            for item in releases
        ):
            raise ValueError(
                "Eurostat international trade in goods release headline evidence differs"
            )
        occurrences = [item.occurrence_key for item in releases]
        if len(occurrences) != len(set(occurrences)):
            raise ValueError(
                "Eurostat international trade in goods releases repeat an occurrence"
            )
        periods = [item.reference_period for item in releases]
        if len(periods) != len(set(periods)):
            raise ValueError(
                "Eurostat international trade in goods releases repeat a month"
            )
        if (
            min(periods)
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_REFERENCE_PERIOD
            or max(periods)
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_REFERENCE_PERIOD
        ):
            raise ValueError(
                "Eurostat international trade in goods reference range differs"
            )
        expected_release_periods = tuple(
            item
            for item in _month_range(
                EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_REFERENCE_PERIOD,
                EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_REFERENCE_PERIOD,
            )
            if item
            not in EUROSTAT_INTERNATIONAL_TRADE_GOODS_INTERNAL_GAP_REFERENCE_PERIODS
        )
        if tuple(periods) != expected_release_periods:
            full_periods = _month_range(
                EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_REFERENCE_PERIOD,
                EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_REFERENCE_PERIOD,
            )
            actual_gaps = tuple(
                item for item in full_periods if item not in periods
            )
            raise ValueError(
                "Eurostat international trade in goods reference-month lineage differs: "
                f"actual gaps={actual_gaps!r}"
            )
        if any(item.inventory_entry.release_date > as_of for item in releases):
            raise ValueError(
                "Eurostat international trade in goods release exceeds archive boundary"
            )
        if self.current_dataset.updated_at[:10] > as_of:
            raise ValueError(
                "Eurostat international trade in goods dataset exceeds archive boundary"
            )
        expected_gaps = (
            *_month_range(
                self.current_dataset.time_start,
                _month_shift(
                    EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_REFERENCE_PERIOD,
                    -1,
                ),
            ),
            *EUROSTAT_INTERNATIONAL_TRADE_GOODS_INTERNAL_GAP_REFERENCE_PERIODS,
        )
        gaps = tuple(
            _month(item, "searchable_release_gap_reference_period")
            for item in self.searchable_release_gap_reference_periods
        )
        if gaps != expected_gaps:
            raise ValueError(
                "Eurostat international trade in goods searchable-release gap inventory differs"
            )
        for name, maximum in (
            (
                "reference_period_inferred_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES,
            ),
            (
                "composition_source_stated_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES,
            ),
            (
                "page_date_offset_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES,
            ),
            (
                "exact_publication_time_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES,
            ),
            (
                "release_document_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES,
            ),
            (
                "published_value_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES
                * MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "revision_comparison_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES
                * MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "changed_revision_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES
                * MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "current_dataset_comparison_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES
                * MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "changed_current_dataset_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASES
                * MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUES_PER_RELEASE,
            ),
        ):
            _bounded_int(getattr(self, name), name, maximum)
        expected_inferred_references = sum(
            not item.reference_period_source_stated for item in releases
        )
        if self.reference_period_inferred_count != expected_inferred_references:
            raise ValueError(
                "Eurostat international trade in goods inferred-reference count differs"
            )
        if self.composition_source_stated_count != sum(
            item.composition_source_stated for item in releases
        ):
            raise ValueError(
                "Eurostat international trade in goods composition count differs"
            )
        expected_offsets = sum(
            item.page_release_date_offset_days != 0 for item in releases
        )
        if self.page_date_offset_count != expected_offsets:
            raise ValueError(
                "Eurostat international trade in goods page-date offset count differs"
            )
        expected_exact_times = sum(
            item.published_at is not None for item in releases
        )
        if self.exact_publication_time_count != expected_exact_times:
            raise ValueError(
                "Eurostat international trade in goods exact publication-time count differs"
            )
        document_artifacts = tuple(
            item.document_artifact
            for item in releases
            if item.document_artifact is not None
        )
        if (
            len(document_artifacts)
            != EUROSTAT_INTERNATIONAL_TRADE_GOODS_DOCUMENT_COUNT
        ):
            raise ValueError(
                "Eurostat international trade in goods English document count differs"
            )
        if self.release_document_count != len(document_artifacts):
            raise ValueError(
                "Eurostat international trade in goods release-document count differs"
            )
        expected_value_count = sum(
            len(item.published_values) for item in releases
        )
        if self.published_value_count != expected_value_count:
            raise ValueError(
                "Eurostat international trade in goods published-value count differs"
            )
        revision_counts = _published_revision_counts(releases)
        if revision_counts != (
            self.revision_comparison_count,
            self.changed_revision_count,
        ):
            raise ValueError(
                "Eurostat international trade in goods revision counts differ"
            )
        dataset_counts = _current_dataset_comparison_counts(
            releases, self.current_dataset
        )
        if dataset_counts != (
            self.current_dataset_comparison_count,
            self.changed_current_dataset_count,
        ):
            raise ValueError(
                "Eurostat international trade in goods dataset comparison differs"
            )
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
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_ARTIFACTS,
            ),
            _bounded_int(
                self.unique_content_sha256_count,
                "unique_content_sha256_count",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_ARTIFACTS,
            ),
            _bounded_int(
                self.total_content_bytes,
                "total_content_bytes",
                MAX_EUROSTAT_INTERNATIONAL_TRADE_GOODS_TOTAL_BYTES,
            ),
        )
        if counts != (
            expected_artifact_count,
            expected_unique_count,
            expected_bytes,
        ):
            raise ValueError(
                "Eurostat international trade in goods artifact totals differ"
            )
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "releases", releases)
        object.__setattr__(
            self, "searchable_release_gap_reference_periods", gaps
        )
        expected = _stable_id(
            "eurostat-international-trade-goods-archive-manifest",
            self.identity_payload(),
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError(
                "Eurostat international trade in goods manifest identity differs"
            )
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
    ) -> EurostatInternationalTradeGoodsArchiveManifestV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            inventory=EurostatInternationalTradeGoodsInventoryV1.from_dict(
                _mapping(data.get("inventory"), "inventory")
            ),
            current_dataset=EurostatInternationalTradeGoodsDatasetV1.from_dict(
                _mapping(data.get("current_dataset"), "current_dataset")
            ),
            releases=tuple(
                EurostatInternationalTradeGoodsReleaseV1.from_dict(
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
    def from_json(
        cls, value: str
    ) -> EurostatInternationalTradeGoodsArchiveManifestV1:
        try:
            data = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "Eurostat international trade in goods manifest is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_eurostat_international_trade_goods_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    dataset_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatInternationalTradeGoodsArchiveManifestV1:
    """Build the international trade in goods receipt from exact retained Eurostat bytes."""
    source = registry.source(EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY)
    if (
        source.parser_id != EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_ID
        or source.parser_version
        != EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_VERSION
    ):
        raise ValueError(
            "Eurostat international trade in goods registry parser identity differs"
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
            "Eurostat international trade in goods snapshot source identity differs"
        )
    inventory = parse_eurostat_international_trade_goods_inventory(
        search_values, as_of_date=as_of_date
    )
    dataset = parse_eurostat_international_trade_goods_dataset(dataset_snapshot)
    snapshots = release_values
    if len(snapshots) != EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_COUNT:
        raise ValueError(
            "Eurostat international trade in goods release snapshot count differs"
        )
    by_code: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        code = _product_code(snapshot.request.uri)
        if code in by_code:
            raise ValueError(
                "Eurostat international trade in goods release corpus repeats a product code"
            )
        by_code[code] = snapshot
    if set(by_code) != {item.product_code for item in inventory.entries}:
        raise ValueError(
            "Eurostat international trade in goods release corpus is incomplete"
        )
    if (
        len(document_values)
        != EUROSTAT_INTERNATIONAL_TRADE_GOODS_DOCUMENT_COUNT
    ):
        raise ValueError(
            "Eurostat international trade in goods document snapshot count differs"
        )
    documents_by_uri: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in document_values:
        if snapshot.request.uri in documents_by_uri:
            raise ValueError(
                "Eurostat international trade in goods document corpus repeats a URI"
            )
        documents_by_uri[snapshot.request.uri] = snapshot
    used_document_uris: set[str] = set()
    parsed: list[
        tuple[
            EurostatInternationalTradeGoodsInventoryEntryV1,
            EurostatInternationalTradeGoodsArtifactV1,
            EurostatInternationalTradeGoodsArtifactV1 | None,
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
                "Eurostat international trade in goods release request identity differs"
            )
        artifact = _artifact_from_snapshot(
            snapshot,
            EurostatInternationalTradeGoodsArtifactRole.RELEASE_HTML,
            product_code=entry.product_code,
        )
        _, page_date, published_at, document_uris = _parse_release_landing(
            snapshot, entry
        )
        document_artifact: EurostatInternationalTradeGoodsArtifactV1 | None = (
            None
        )
        document_snapshot: OfficialRawSnapshotV1 | None = None
        if document_uris:
            document_uri = document_uris[0]
            try:
                document_snapshot = documents_by_uri[document_uri]
            except KeyError as exc:
                raise ValueError(
                    "Eurostat international trade in goods document corpus is incomplete"
                ) from exc
            used_document_uris.add(document_uri)
            document_role = {
                OfficialSourceFormat.HTML: (
                    EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_HTML
                ),
                OfficialSourceFormat.PDF: (
                    EurostatInternationalTradeGoodsArtifactRole.RELEASE_DOCUMENT_PDF
                ),
            }.get(document_snapshot.request.source_format)
            if document_role is None:
                raise ValueError(
                    "Eurostat international trade in goods document format differs"
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
            "Eurostat international trade in goods document corpus has unexpected artifacts"
        )
    releases: list[EurostatInternationalTradeGoodsReleaseV1] = []
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
                "Eurostat international trade in goods release lag changed during parsing"
            )
        headline = _headline_value(entry, reference_period)
        document_snapshot = (
            documents_by_uri[document_uris[0]] if document_uris else None
        )
        releases.append(
            EurostatInternationalTradeGoodsReleaseV1(
                inventory_entry=entry,
                reference_period=reference_period,
                reference_period_source_stated=(
                    entry.product_code
                    not in EUROSTAT_INTERNATIONAL_TRADE_GOODS_REFERENCE_PERIOD_INFERENCES
                ),
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
    return EurostatInternationalTradeGoodsArchiveManifestV1(
        as_of_date=as_of_date,
        registry_id=registry.registry_id,
        source_id=source.source_id,
        inventory=inventory,
        current_dataset=dataset,
        releases=tuple(releases),
        searchable_release_gap_reference_periods=(
            *_month_range(
                dataset.time_start,
                _month_shift(
                    EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_REFERENCE_PERIOD,
                    -1,
                ),
            ),
            *EUROSTAT_INTERNATIONAL_TRADE_GOODS_INTERNAL_GAP_REFERENCE_PERIODS,
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


def replay_eurostat_international_trade_goods_archive(
    registry: OfficialSourceRegistryV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    dataset_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Sequence[OfficialRawSnapshotV1],
    expected_manifest: EurostatInternationalTradeGoodsArchiveManifestV1,
) -> EurostatInternationalTradeGoodsArchiveManifestV1:
    """Rebuild the international trade in goods receipt and require byte-for-byte identity."""
    rebuilt = build_eurostat_international_trade_goods_archive_manifest(
        registry,
        search_snapshots,
        dataset_snapshot,
        release_snapshots,
        document_snapshots,
        as_of_date=expected_manifest.as_of_date,
    )
    if rebuilt.to_json() != expected_manifest.to_json():
        raise ValueError(
            "Eurostat international trade in goods replay differs from packaged manifest"
        )
    return rebuilt


def packaged_eurostat_international_trade_goods_manifest_path() -> Path:
    """Return the installed international trade in goods archive-manifest path."""
    return (
        Path(__file__).resolve().parent
        / "assets"
        / "eurostat_international_trade_goods_archive_v1.json"
    )


def load_packaged_eurostat_international_trade_goods_archive_manifest() -> (
    EurostatInternationalTradeGoodsArchiveManifestV1
):
    """Load and validate the installed international trade in goods archive manifest."""
    path = packaged_eurostat_international_trade_goods_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError(
            "packaged Eurostat international trade in goods manifest exceeds size bound"
        )
    manifest = EurostatInternationalTradeGoodsArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY)
    if (
        manifest.registry_id != registry.registry_id
        or manifest.source_id != source.source_id
    ):
        raise ValueError(
            "packaged Eurostat international trade in goods registry binding differs"
        )
    return manifest


__all__ = [
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_ARTIFACT_SCHEMA_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_AS_OF_DATE",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_CURRENT_DATASET_UNIT",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_LABEL",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_SCHEMA_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_DATASET_URI",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_DOCUMENT_COUNT",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_EXCLUSION_SCHEMA_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_REFERENCE_PERIOD",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_FIRST_SEARCH_RELEASE_DATE",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_INTERNAL_GAP_REFERENCE_PERIODS",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_INVENTORY_ENTRY_SCHEMA_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_INVENTORY_SCHEMA_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_PACKAGED_RELEASE_DATE",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_LATEST_REFERENCE_PERIOD",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_MANIFEST_SCHEMA_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_MEASURE_KEY",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_OBSERVATION_SCHEMA_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_PAGE_DATE_OFFSET_EXCEPTIONS",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_ID",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_PARSER_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_PROGRAM_KEY",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_PUBLISHED_VALUE_SCHEMA_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_REFERENCE_PERIOD_INFERENCES",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_COUNT",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_SCHEMA_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_UNIT",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_RELEASE_VALUE_SCHEMA_VERSION",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_PAGE_COUNT",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_RESULT_COUNT",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_UNIQUE_URI_COUNT",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_SEARCH_URI",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_KEY",
    "EUROSTAT_INTERNATIONAL_TRADE_GOODS_SOURCE_TIMEZONE",
    "EurostatInternationalTradeGoodsArchiveManifestV1",
    "EurostatInternationalTradeGoodsArtifactRole",
    "EurostatInternationalTradeGoodsArtifactV1",
    "EurostatInternationalTradeGoodsDatasetV1",
    "EurostatInternationalTradeGoodsIndexExclusionV1",
    "EurostatInternationalTradeGoodsInventoryEntryV1",
    "EurostatInternationalTradeGoodsInventoryV1",
    "EurostatInternationalTradeGoodsMeasure",
    "EurostatInternationalTradeGoodsMovement",
    "EurostatInternationalTradeGoodsObservationV1",
    "EurostatInternationalTradeGoodsPublishedValueV1",
    "EurostatInternationalTradeGoodsReleaseV1",
    "EurostatInternationalTradeGoodsReleaseValueV1",
    "build_eurostat_international_trade_goods_archive_manifest",
    "build_eurostat_international_trade_goods_dataset_request",
    "build_eurostat_international_trade_goods_document_requests",
    "build_eurostat_international_trade_goods_release_requests",
    "build_eurostat_international_trade_goods_search_requests",
    "load_packaged_eurostat_international_trade_goods_archive_manifest",
    "packaged_eurostat_international_trade_goods_manifest_path",
    "parse_eurostat_international_trade_goods_dataset",
    "parse_eurostat_international_trade_goods_inventory",
    "replay_eurostat_international_trade_goods_archive",
]
