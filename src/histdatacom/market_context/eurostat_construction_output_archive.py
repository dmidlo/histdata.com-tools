"""Deterministic qualification of Eurostat construction-output releases.

Eurostat's searchable Euro-indicator feed is the release inventory, while
source-dated HTML/PDF products are the vintage evidence.  The current
``sts_copr_m`` JSON-stat table is retained only as a revised-series cross-check;
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

EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_KEY: Final = (
    "ea.eurostat.construction-output"
)
EUROSTAT_CONSTRUCTION_OUTPUT_PROGRAM_KEY: Final = (
    "ea.eurostat.construction-output"
)
EUROSTAT_CONSTRUCTION_OUTPUT_MEASURE_KEY: Final = (
    "construction-output-total-construction"
)
EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_ID: Final = (
    "official.eurostat-construction-output.v1"
)
EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_VERSION: Final = "1"
EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_TIMEZONE: Final = "Europe/Brussels"
EUROSTAT_CONSTRUCTION_OUTPUT_AS_OF_DATE: Final = "2026-09-18"
EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_SEARCH_RELEASE_DATE: Final = "2002-03-13"
EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_PACKAGED_RELEASE_DATE: Final = "2026-09-18"
EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_REFERENCE_PERIOD: Final = "2001-Q4"
EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_REFERENCE_PERIOD: Final = "2026-07"
EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_MONTHLY_REFERENCE_PERIOD: Final = "2006-11"
EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_MONTHLY_RELEASE_DATE: Final = "2007-01-18"
EUROSTAT_CONSTRUCTION_OUTPUT_LAST_QUARTERLY_REFERENCE_PERIOD: Final = "2006-Q3"
EUROSTAT_CONSTRUCTION_OUTPUT_INTERNAL_GAP_REFERENCE_PERIODS: Final = (
    "2015-04",
)
EUROSTAT_CONSTRUCTION_OUTPUT_REPEATED_REFERENCE_PERIODS: Final = ("2002-Q4",)
EUROSTAT_CONSTRUCTION_OUTPUT_QUARTERLY_RELEASE_COUNT: Final = 21
EUROSTAT_CONSTRUCTION_OUTPUT_MONTHLY_RELEASE_COUNT: Final = 236
EUROSTAT_CONSTRUCTION_OUTPUT_PAGE_DATE_OFFSET_EXCEPTIONS: Final = {}
EUROSTAT_CONSTRUCTION_OUTPUT_REFERENCE_PERIOD_INFERENCES: Final = {}
EUROSTAT_CONSTRUCTION_OUTPUT_PERIOD_HEADER_CORRECTIONS: Final = {
    "4-19092011-ap": (
        (
            "2010-Q3",
            "2010-Q4",
            "2011-Q1",
            "2011-Q2",
            "2011-02",
            "2011-03",
            "2011-04",
            "2011-06",
            "2011-06",
            "2011-07",
        ),
        (
            "2010-Q3",
            "2010-Q4",
            "2011-Q1",
            "2011-Q2",
            "2011-02",
            "2011-03",
            "2011-04",
            "2011-05",
            "2011-06",
            "2011-07",
        ),
    )
}
EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT: Final = 10
EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_RESULT_COUNT: Final = 916
EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_UNIQUE_URI_COUNT: Final = 915
EUROSTAT_CONSTRUCTION_OUTPUT_TITLE_MATCH_COUNT: Final = 258
EUROSTAT_CONSTRUCTION_OUTPUT_PREFILTER_COUNT: Final = 257
EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_COUNT: Final = 257
EUROSTAT_CONSTRUCTION_OUTPUT_DOCUMENT_COUNT: Final = 225

EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_URI: Final = (
    "https://ec.europa.eu/eurostat/search"
)
EUROSTAT_CONSTRUCTION_OUTPUT_PRODUCT_URI_TEMPLATE: Final = (
    "https://ec.europa.eu/eurostat/product?code={product_code}"
)
EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_URI: Final = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "sts_copr_m?lang=en&indic_bt=PRD&nace_r2=F&"
    "s_adj=SCA&s_adj=CA&unit=PCH_PRE&unit=PCH_SM&"
    "geo=EA21&geo=DE&geo=FR&sinceTimePeriod=2000-01"
)

EUROSTAT_CONSTRUCTION_OUTPUT_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-construction-output-artifact.v1"
)
EUROSTAT_CONSTRUCTION_OUTPUT_INVENTORY_ENTRY_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-construction-output-inventory-entry.v1"
)
EUROSTAT_CONSTRUCTION_OUTPUT_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-construction-output-exclusion.v1"
)
EUROSTAT_CONSTRUCTION_OUTPUT_INVENTORY_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-construction-output-inventory.v1"
)
EUROSTAT_CONSTRUCTION_OUTPUT_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-construction-output-observation.v1"
)
EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-construction-output-dataset.v1"
)
EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-construction-output-release-value.v1"
)
EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-construction-output-published-value.v1"
)
EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-construction-output-release.v1"
)
EUROSTAT_CONSTRUCTION_OUTPUT_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.eurostat-construction-output-archive-manifest.v1"
)

MAX_EUROSTAT_CONSTRUCTION_OUTPUT_INDEX_PAGES: Final = 32
MAX_EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_ENTRIES: Final = 2_048
MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES: Final = 512
MAX_EUROSTAT_CONSTRUCTION_OUTPUT_OBSERVATIONS: Final = 4_096
MAX_EUROSTAT_CONSTRUCTION_OUTPUT_ARTIFACTS: Final = 2_048
MAX_EUROSTAT_CONSTRUCTION_OUTPUT_TOTAL_BYTES: Final = 1_024_000_000
MAX_EUROSTAT_CONSTRUCTION_OUTPUT_TITLE_CHARS: Final = 512
MAX_EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUES_PER_RELEASE: Final = 96

_SEARCH_PREFIX: Final = (
    "_estatsearchportlet_WAR_estatsearchportlet_INSTANCE_bHVzuvn1SZ8J_"
)
_ATOM_NAMESPACE: Final = "http://www.w3.org/2005/Atom"
_PRODUCT_CODE_RE = re.compile(
    r"(?P<prefix>[1-9])-(?P<day>\d{2})(?P<month>\d{2})(?P<year>0?\d{4})-"
    r"(?P<suffix>ap(?:\d+|_\d+)?|bp\d*|cp\d*)",
    re.IGNORECASE,
)
_PRODUCT_RELEASE_DATE_OVERRIDES: Final = {
    # Broad-search exclusion with a legacy language token inside the code.
    "4-17112004-de-bp": "2004-11-17",
}
_MONTH_RE = re.compile(r"\d{4}-(?:0[1-9]|1[0-2])")
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


def _period(value: object, name: str) -> str:
    """Validate a source-stated monthly or quarterly reference period."""
    result = _required_text(value, name)
    if _MONTH_RE.fullmatch(result) is not None:
        return _month(result, name)
    if _QUARTER_RE.fullmatch(result) is not None:
        return result
    raise ValueError(f"{name} must be YYYY-MM or YYYY-Qn")


def _period_frequency(value: str) -> str:
    token = _period(value, "reference_period")
    return "quarterly" if _QUARTER_RE.fullmatch(token) else "monthly"


def _period_end(value: str) -> date:
    token = _period(value, "reference_period")
    if _MONTH_RE.fullmatch(token) is not None:
        year, month = (int(item) for item in token.split("-"))
    else:
        year = int(token[:4])
        month = int(token[-1]) * 3
    return date(year, month, monthrange(year, month)[1])


def _period_shift(value: str, offset: int) -> str:
    token = _period(value, "period")
    if _MONTH_RE.fullmatch(token) is not None:
        return _month_shift(token, offset)
    year = int(token[:4])
    quarter = int(token[-1])
    ordinal = year * 4 + quarter - 1 + offset
    return f"{ordinal // 4:04d}-Q{ordinal % 4 + 1}"


def _period_range(start: str, end: str) -> tuple[str, ...]:
    first = _period(start, "start")
    last = _period(end, "end")
    if _period_frequency(first) != _period_frequency(last):
        raise ValueError("period range cannot mix monthly and quarterly values")
    if last < first:
        return ()
    result: list[str] = []
    current = first
    while current <= last:
        result.append(current)
        current = _period_shift(current, 1)
    return tuple(result)


def _period_sort_key(value: str) -> tuple[date, str]:
    token = _period(value, "reference_period")
    return _period_end(token), token


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
                "Eurostat construction output product URI has no unique product code"
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
        raise ValueError("Eurostat construction output product code is invalid")
    return match.group(0).casefold()


def _product_release_date(product_code: object) -> str:
    normalized = _product_code(product_code)
    override = _PRODUCT_RELEASE_DATE_OVERRIDES.get(normalized)
    if override is not None:
        return override
    match = _PRODUCT_CODE_RE.fullmatch(normalized)
    if match is None:  # pragma: no cover - guarded by _product_code
        raise ValueError("Eurostat construction output product code is invalid")
    token = f"{int(match.group('year')):04d}-{match.group('month')}-{match.group('day')}"
    return _iso_date(token, "release_date")


def _inventory_prefilter(title: str) -> bool:
    normalized = _required_text(title, "title").casefold()
    return bool(
        normalized.startswith(
            (
                "production in construction ",
                "construction output ",
                "production in the construction sector ",
            )
        )
        or re.match(
            r"^euro area(?: and eu\d+)? production in construction\b",
            normalized,
        )
    )


def _inventory_selected(title: str, product_code: str) -> bool:
    _product_code(product_code)
    return _inventory_prefilter(title)


def _exclusion_reason(title: str) -> str:
    normalized = _required_text(title, "title").casefold()
    if "construction" not in normalized:
        return "broad search-summary match without construction in the title"
    if normalized.startswith(
        "engineering, manufacturing and construction dominated"
    ):
        return "education article, not a construction-output release"
    if normalized.startswith("industrial producer prices"):
        return "industrial producer-price release, not production volume"
    if normalized.startswith("services production"):
        return "services-production release, not construction output"
    if "gdp" in normalized or "employment" in normalized:
        return "national-accounts or labour release, not construction output"
    if "trade" in normalized:
        return "external-trade release, not construction output"
    return "broad search-summary match outside the construction-output headline lineage"


class EurostatConstructionOutputArtifactRole(str, Enum):
    """Role of one exact official byte artifact."""

    SEARCH_INDEX = "search-index"
    CURRENT_DATASET = "current-dataset"
    RELEASE_HTML = "release-html"
    RELEASE_DOCUMENT_HTML = "release-document-html"
    RELEASE_DOCUMENT_PDF = "release-document-pdf"

    @classmethod
    def from_value(
        cls, value: str | EurostatConstructionOutputArtifactRole
    ) -> EurostatConstructionOutputArtifactRole:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat construction output artifact role"
            ) from exc


class EurostatConstructionOutputMeasure(str, Enum):
    """Qualified construction output statistic carried by this archive."""

    MONTH_OVER_MONTH = "month-over-month-percent"
    QUARTER_OVER_QUARTER = "quarter-over-quarter-percent"
    YEAR_OVER_YEAR = "year-over-year-percent"

    @classmethod
    def from_value(
        cls, value: str | EurostatConstructionOutputMeasure
    ) -> EurostatConstructionOutputMeasure:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat construction output measure"
            ) from exc


_CURRENT_DATASET_MEASURES: Final = (
    EurostatConstructionOutputMeasure.MONTH_OVER_MONTH,
    EurostatConstructionOutputMeasure.YEAR_OVER_YEAR,
)


@dataclass(frozen=True, slots=True)
class EurostatConstructionOutputArtifactV1:
    """Content-addressed official construction output evidence artifact."""

    role: EurostatConstructionOutputArtifactRole
    request_uri: str
    resolved_uri: str
    source_format: OfficialSourceFormat
    content_length: int
    content_sha256: str
    page_number: int | None = None
    product_code: str | None = None
    schema_version: str = EUROSTAT_CONSTRUCTION_OUTPUT_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_CONSTRUCTION_OUTPUT_ARTIFACT_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat construction output artifact schema"
            )
        role = EurostatConstructionOutputArtifactRole.from_value(self.role)
        request_uri = _https_uri(self.request_uri, "request_uri")
        resolved_uri = _https_uri(self.resolved_uri, "resolved_uri")
        source_format = OfficialSourceFormat.from_value(self.source_format)
        expected_format = {
            EurostatConstructionOutputArtifactRole.SEARCH_INDEX: OfficialSourceFormat.ATOM,
            EurostatConstructionOutputArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
            EurostatConstructionOutputArtifactRole.RELEASE_HTML: OfficialSourceFormat.HTML,
            EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
            EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
        }[role]
        if source_format is not expected_format:
            raise ValueError(
                "Eurostat construction output artifact role and format differ"
            )
        length = _bounded_int(
            self.content_length,
            "content_length",
            MAX_EUROSTAT_CONSTRUCTION_OUTPUT_TOTAL_BYTES,
        )
        if length < 1:
            raise ValueError("Eurostat construction output artifact is empty")
        page_number = self.page_number
        product_code = self.product_code
        if role is EurostatConstructionOutputArtifactRole.SEARCH_INDEX:
            if page_number is None:
                raise ValueError(
                    "Eurostat construction output search artifact needs a page number"
                )
            _bounded_int(
                page_number,
                "page_number",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_INDEX_PAGES,
            )
            if product_code is not None:
                raise ValueError(
                    "Eurostat construction output search artifact has a product code"
                )
        elif role is EurostatConstructionOutputArtifactRole.CURRENT_DATASET:
            if page_number is not None or product_code is not None:
                raise ValueError(
                    "Eurostat construction output dataset artifact has release metadata"
                )
        else:
            if page_number is not None or product_code is None:
                raise ValueError(
                    "Eurostat construction output release artifact metadata differs"
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
    ) -> EurostatConstructionOutputArtifactV1:
        return cls(
            role=EurostatConstructionOutputArtifactRole.from_value(
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
class EurostatConstructionOutputInventoryEntryV1:
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
        EUROSTAT_CONSTRUCTION_OUTPUT_INVENTORY_ENTRY_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_CONSTRUCTION_OUTPUT_INVENTORY_ENTRY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat construction output inventory-entry schema"
            )
        product_code = _product_code(self.product_code)
        release_date = _iso_date(self.release_date, "release_date")
        if release_date != _product_release_date(product_code):
            raise ValueError(
                "Eurostat construction output release date differs from product code"
            )
        title = _required_text(
            self.source_title,
            "source_title",
            maximum=MAX_EUROSTAT_CONSTRUCTION_OUTPUT_TITLE_CHARS,
        )
        if not _inventory_selected(title, product_code):
            raise ValueError(
                "Eurostat construction output inventory entry is outside selection"
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
                "Eurostat construction output source URI route differs"
            )
        if _product_code(source_uri) != product_code:
            raise ValueError(
                "Eurostat construction output source URI and product code differ"
            )
        page = _bounded_int(
            self.page_number,
            "page_number",
            MAX_EUROSTAT_CONSTRUCTION_OUTPUT_INDEX_PAGES,
        )
        if page < 1:
            raise ValueError(
                "Eurostat construction output page number must be positive"
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
            "eurostat-construction-output-inventory-entry",
            self.identity_payload(),
        )
        if self.entry_id and self.entry_id != expected:
            raise ValueError(
                "Eurostat construction output inventory-entry identity differs"
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
    ) -> EurostatConstructionOutputInventoryEntryV1:
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
class EurostatConstructionOutputIndexExclusionV1:
    """One explicit non-lineage result from the bounded Atom search."""

    product_code: str
    release_date: str
    source_title: str
    source_uri: str
    reason: str
    exclusion_id: str = ""
    schema_version: str = EUROSTAT_CONSTRUCTION_OUTPUT_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_CONSTRUCTION_OUTPUT_EXCLUSION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat construction output exclusion schema"
            )
        product_code = _product_code(self.product_code)
        release_date = _iso_date(self.release_date, "release_date")
        if release_date != _product_release_date(product_code):
            raise ValueError(
                "Eurostat construction output exclusion date differs"
            )
        title = _required_text(
            self.source_title,
            "source_title",
            maximum=MAX_EUROSTAT_CONSTRUCTION_OUTPUT_TITLE_CHARS,
        )
        if _inventory_selected(title, product_code):
            raise ValueError(
                "Eurostat construction output exclusion matches selection"
            )
        source_uri = _https_uri(self.source_uri, "source_uri")
        if _product_code(source_uri) != product_code:
            raise ValueError(
                "Eurostat construction output exclusion URI differs"
            )
        reason = _required_text(self.reason, "reason")
        if reason != _exclusion_reason(title):
            raise ValueError(
                "Eurostat construction output exclusion reason differs"
            )
        object.__setattr__(self, "product_code", product_code)
        object.__setattr__(self, "release_date", release_date)
        object.__setattr__(self, "source_title", title)
        object.__setattr__(self, "source_uri", source_uri)
        object.__setattr__(self, "reason", reason)
        expected = _stable_id(
            "eurostat-construction-output-exclusion", self.identity_payload()
        )
        if self.exclusion_id and self.exclusion_id != expected:
            raise ValueError(
                "Eurostat construction output exclusion identity differs"
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
    ) -> EurostatConstructionOutputIndexExclusionV1:
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
class EurostatConstructionOutputInventoryV1:
    """Hash-bound official search inventory and deterministic selection."""

    artifacts: tuple[EurostatConstructionOutputArtifactV1, ...]
    entries: tuple[EurostatConstructionOutputInventoryEntryV1, ...]
    exclusions: tuple[EurostatConstructionOutputIndexExclusionV1, ...]
    query_result_count: int
    unique_uri_count: int
    title_match_count: int
    prefilter_count: int
    inventory_id: str = ""
    schema_version: str = EUROSTAT_CONSTRUCTION_OUTPUT_INVENTORY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_CONSTRUCTION_OUTPUT_INVENTORY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat construction output inventory schema"
            )
        artifacts = tuple(self.artifacts)
        entries = tuple(self.entries)
        exclusions = tuple(self.exclusions)
        if len(artifacts) != EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT:
            raise ValueError(
                "Eurostat construction output search page count differs"
            )
        if any(
            item.role is not EurostatConstructionOutputArtifactRole.SEARCH_INDEX
            for item in artifacts
        ):
            raise ValueError(
                "Eurostat construction output inventory artifact role differs"
            )
        if [item.page_number for item in artifacts] != list(
            range(1, EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT + 1)
        ):
            raise ValueError(
                "Eurostat construction output search pages are not contiguous"
            )
        if any(
            item.request_uri != _search_uri(cast(int, item.page_number))
            or item.resolved_uri != item.request_uri
            for item in artifacts
        ):
            raise ValueError(
                "Eurostat construction output search artifact identity differs"
            )
        if len(entries) != EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_COUNT:
            raise ValueError(
                "Eurostat construction output selected release count differs"
            )
        codes = [item.product_code for item in entries]
        if len(codes) != len(set(codes)):
            raise ValueError(
                "Eurostat construction output inventory repeats a product code"
            )
        if list(entries) != sorted(
            entries, key=lambda item: (item.release_date, item.product_code)
        ):
            raise ValueError(
                "Eurostat construction output inventory entries are not canonical"
            )
        if (
            entries[0].release_date
            != EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_SEARCH_RELEASE_DATE
        ):
            raise ValueError(
                "Eurostat construction output first searchable release differs"
            )
        if (
            entries[-1].release_date
            != EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_PACKAGED_RELEASE_DATE
        ):
            raise ValueError(
                "Eurostat construction output latest searchable release differs"
            )
        if len(exclusions) != (
            EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_UNIQUE_URI_COUNT
            - EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_COUNT
        ):
            raise ValueError(
                "Eurostat construction output exclusion count differs"
            )
        exclusion_codes = [item.product_code for item in exclusions]
        if len(exclusion_codes) != len(set(exclusion_codes)):
            raise ValueError(
                "Eurostat construction output exclusions repeat a product"
            )
        if list(exclusions) != sorted(
            exclusions, key=lambda item: (item.release_date, item.product_code)
        ):
            raise ValueError(
                "Eurostat construction output exclusions are not canonical"
            )
        if set(codes) & set(exclusion_codes):
            raise ValueError(
                "Eurostat construction output selection includes an exclusion"
            )
        expected_counts = (
            EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_RESULT_COUNT,
            EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_UNIQUE_URI_COUNT,
            EUROSTAT_CONSTRUCTION_OUTPUT_TITLE_MATCH_COUNT,
            EUROSTAT_CONSTRUCTION_OUTPUT_PREFILTER_COUNT,
        )
        observed_counts = (
            _bounded_int(
                self.query_result_count,
                "query_result_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.unique_uri_count,
                "unique_uri_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.title_match_count,
                "title_match_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_ENTRIES,
            ),
            _bounded_int(
                self.prefilter_count,
                "prefilter_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_ENTRIES,
            ),
        )
        if observed_counts != expected_counts:
            raise ValueError(
                "Eurostat construction output search inventory counts differ"
            )
        object.__setattr__(self, "artifacts", artifacts)
        object.__setattr__(self, "entries", entries)
        object.__setattr__(self, "exclusions", exclusions)
        expected = _stable_id(
            "eurostat-construction-output-inventory", self.identity_payload()
        )
        if self.inventory_id and self.inventory_id != expected:
            raise ValueError(
                "Eurostat construction output inventory identity differs"
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
    ) -> EurostatConstructionOutputInventoryV1:
        return cls(
            artifacts=tuple(
                EurostatConstructionOutputArtifactV1.from_dict(
                    _mapping(item, "artifact")
                )
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            entries=tuple(
                EurostatConstructionOutputInventoryEntryV1.from_dict(
                    _mapping(item, "inventory entry")
                )
                for item in _sequence(data.get("entries"), "entries")
            ),
            exclusions=tuple(
                EurostatConstructionOutputIndexExclusionV1.from_dict(
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
class EurostatConstructionOutputObservationV1:
    """One current-revised JSON-stat construction-output observation."""

    economy_code: str
    reference_period: str
    measure: EurostatConstructionOutputMeasure
    value_lexical: str
    value: float
    status: str | None
    flat_index: int
    observation_id: str = ""
    schema_version: str = (
        EUROSTAT_CONSTRUCTION_OUTPUT_OBSERVATION_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_CONSTRUCTION_OUTPUT_OBSERVATION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat construction output observation schema"
            )
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _ECONOMIES:
            raise ValueError(
                "Eurostat construction output observation economy differs"
            )
        reference = _month(self.reference_period, "reference_period")
        measure = EurostatConstructionOutputMeasure.from_value(self.measure)
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError(
                "Eurostat construction output observation lexical is invalid"
            )
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError(
                "Eurostat construction output observation numeric differs"
            )
        if not -250.0 <= numeric <= 250.0:
            raise ValueError(
                "Eurostat construction output observation is outside its bound"
            )
        status = _optional_text(self.status, "status")
        if status is not None and len(status) > 16:
            raise ValueError(
                "Eurostat construction output status exceeds its bound"
            )
        flat_index = _bounded_int(
            self.flat_index,
            "flat_index",
            MAX_EUROSTAT_CONSTRUCTION_OUTPUT_OBSERVATIONS * 4,
        )
        object.__setattr__(self, "economy_code", economy)
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "flat_index", flat_index)
        expected = _stable_id(
            "eurostat-construction-output-observation",
            self.identity_payload(),
        )
        if self.observation_id and self.observation_id != expected:
            raise ValueError(
                "Eurostat construction output observation identity differs"
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
    ) -> EurostatConstructionOutputObservationV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatConstructionOutputMeasure.from_value(
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
class EurostatConstructionOutputDatasetV1:
    """Exact current-revised ``sts_copr_m`` cross-check response."""

    label: str
    updated_at: str
    time_start: str
    time_end: str
    artifact: EurostatConstructionOutputArtifactV1
    observations: tuple[EurostatConstructionOutputObservationV1, ...]
    dataset_id: str = "sts_copr_m"
    dataset_receipt_id: str = ""
    schema_version: str = EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat construction output dataset schema"
            )
        if self.dataset_id != "sts_copr_m":
            raise ValueError("unsupported Eurostat construction output dataset")
        label = _required_text(self.label, "label")
        updated = _iso_datetime(self.updated_at, "updated_at")
        time_start = _month(self.time_start, "time_start")
        time_end = _month(self.time_end, "time_end")
        if time_end < time_start:
            raise ValueError(
                "Eurostat construction output dataset time range is reversed"
            )
        if (
            self.artifact.role
            is not EurostatConstructionOutputArtifactRole.CURRENT_DATASET
        ):
            raise ValueError(
                "Eurostat construction output dataset artifact role differs"
            )
        if (
            self.artifact.request_uri
            != EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_URI
            or self.artifact.resolved_uri
            != EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_URI
        ):
            raise ValueError(
                "Eurostat construction output dataset artifact identity differs"
            )
        observations = tuple(self.observations)
        if (
            not 1
            <= len(observations)
            <= MAX_EUROSTAT_CONSTRUCTION_OUTPUT_OBSERVATIONS
        ):
            raise ValueError(
                "Eurostat construction output observation count is invalid"
            )
        keys = [item.key for item in observations]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "Eurostat construction output dataset repeats an observation"
            )
        periods = _month_range(time_start, time_end)
        expected_keys = {
            (measure.value, economy, period)
            for measure in _CURRENT_DATASET_MEASURES
            for economy in _ECONOMIES
            for period in periods
        }
        missing_keys = expected_keys - set(keys)
        if missing_keys or set(keys) - expected_keys:
            raise ValueError(
                "Eurostat construction output dataset cube coverage differs"
            )
        period_positions = {
            period: position for position, period in enumerate(periods)
        }
        measure_coordinates = {
            EurostatConstructionOutputMeasure.MONTH_OVER_MONTH: (1, 0),
            EurostatConstructionOutputMeasure.YEAR_OVER_YEAR: (0, 1),
        }
        if any(
            item.flat_index
            != (
                measure_coordinates[item.measure][0]
                * 2
                * len(_ECONOMIES)
                * len(periods)
                + measure_coordinates[item.measure][1]
                * len(_ECONOMIES)
                * len(periods)
                + _ECONOMY_ORDER[item.economy_code] * len(periods)
                + period_positions[item.reference_period]
            )
            for item in observations
        ):
            raise ValueError(
                "Eurostat construction output observation coordinate differs"
            )
        if list(observations) != sorted(
            observations,
            key=lambda item: (
                list(EurostatConstructionOutputMeasure).index(item.measure),
                _ECONOMY_ORDER[item.economy_code],
                item.reference_period,
            ),
        ):
            raise ValueError(
                "Eurostat construction output observations are not canonical"
            )
        if (
            min(item.reference_period for item in observations) != time_start
            or max(item.reference_period for item in observations) != time_end
        ):
            raise ValueError(
                "Eurostat construction output dataset time range differs"
            )
        object.__setattr__(self, "label", label)
        object.__setattr__(self, "updated_at", updated)
        object.__setattr__(self, "time_start", time_start)
        object.__setattr__(self, "time_end", time_end)
        object.__setattr__(self, "observations", observations)
        expected = _stable_id(
            "eurostat-construction-output-dataset", self.identity_payload()
        )
        if self.dataset_receipt_id and self.dataset_receipt_id != expected:
            raise ValueError(
                "Eurostat construction output dataset identity differs"
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
    ) -> EurostatConstructionOutputDatasetV1:
        return cls(
            label=str(data.get("label", "")),
            updated_at=str(data.get("updated_at", "")),
            time_start=str(data.get("time_start", "")),
            time_end=str(data.get("time_end", "")),
            artifact=EurostatConstructionOutputArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            observations=tuple(
                EurostatConstructionOutputObservationV1.from_dict(
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
        MAX_EUROSTAT_CONSTRUCTION_OUTPUT_INDEX_PAGES,
    )
    if page < 1:
        raise ValueError(
            "Eurostat construction output search page must be positive"
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
        f"{_SEARCH_PREFIX}text": "production in construction",
        f"{_SEARCH_PREFIX}sort": "date",
        f"{_SEARCH_PREFIX}collection": "CAT_PREREL",
        f"{_SEARCH_PREFIX}priv_r_p_implicitModel": "true",
        "pageNumber": str(page),
    }
    return f"{EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_URI}?{urlencode(params)}"


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    page_number: int | None = None,
) -> OfficialSourceRequestV1:
    source = registry.source(EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_KEY)
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


def build_eurostat_construction_output_search_requests(
    registry: OfficialSourceRegistryV1,
    *,
    page_count: int = EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the bounded official Atom inventory requests."""
    count = _bounded_int(
        page_count, "page_count", MAX_EUROSTAT_CONSTRUCTION_OUTPUT_INDEX_PAGES
    )
    if count < 1:
        raise ValueError(
            "Eurostat construction output search needs at least one page"
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


def build_eurostat_construction_output_dataset_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact current-revised construction output cross-check request."""
    return _request(
        registry,
        EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_URI,
        OfficialSourceFormat.JSON_STAT,
    )


def build_eurostat_construction_output_release_requests(
    registry: OfficialSourceRegistryV1,
    inventory: EurostatConstructionOutputInventoryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one primary request for every selected construction output product."""
    if not isinstance(inventory, EurostatConstructionOutputInventoryV1):
        raise TypeError(
            "Eurostat construction output release requests require a v1 inventory"
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
        "Eurostat construction output release document format differs"
    )


def build_eurostat_construction_output_document_requests(
    registry: OfficialSourceRegistryV1,
    inventory: EurostatConstructionOutputInventoryV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Discover one English source document for each legacy landing."""
    if not isinstance(inventory, EurostatConstructionOutputInventoryV1):
        raise TypeError(
            "Eurostat construction output document requests require a v1 inventory"
        )
    entries = {item.product_code: item for item in inventory.entries}
    snapshots = tuple(release_snapshots)
    if len(snapshots) != EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_COUNT:
        raise ValueError(
            "Eurostat construction output landing snapshot count differs"
        )
    by_code: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        code = _product_code(snapshot.request.uri)
        if code in by_code:
            raise ValueError(
                "Eurostat construction output landing corpus repeats a product code"
            )
        by_code[code] = snapshot
    if set(by_code) != set(entries):
        raise ValueError(
            "Eurostat construction output landing corpus is incomplete"
        )
    requests: list[OfficialSourceRequestV1] = []
    for code, entry in entries.items():
        snapshot = by_code[code]
        if snapshot.request.uri != entry.source_uri:
            raise ValueError(
                "Eurostat construction output landing request identity differs"
            )
        _, _, _, document_uris = _parse_release_landing(snapshot, entry)
        requests.extend(
            _request(registry, uri, _document_source_format(uri))
            for uri in document_uris
        )
    if (
        EUROSTAT_CONSTRUCTION_OUTPUT_DOCUMENT_COUNT
        and len(requests) != EUROSTAT_CONSTRUCTION_OUTPUT_DOCUMENT_COUNT
    ):
        raise ValueError(
            "Eurostat construction output English document count differs"
        )
    return tuple(requests)


def _artifact_from_snapshot(
    snapshot: OfficialRawSnapshotV1,
    role: EurostatConstructionOutputArtifactRole,
    *,
    product_code: str | None = None,
) -> EurostatConstructionOutputArtifactV1:
    if not isinstance(snapshot, OfficialRawSnapshotV1):
        raise TypeError(
            "Eurostat construction output artifact requires a v1 snapshot"
        )
    if (
        snapshot.request.source_key != EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_KEY
        or snapshot.request.parser_id != EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_ID
        or snapshot.request.parser_version
        != EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_VERSION
    ):
        raise ValueError(
            "Eurostat construction output snapshot source or parser differs"
        )
    if snapshot.request.method is not OfficialRequestMethod.GET:
        raise ValueError("Eurostat construction output snapshot method differs")
    expected_format = {
        EurostatConstructionOutputArtifactRole.SEARCH_INDEX: OfficialSourceFormat.ATOM,
        EurostatConstructionOutputArtifactRole.CURRENT_DATASET: OfficialSourceFormat.JSON_STAT,
        EurostatConstructionOutputArtifactRole.RELEASE_HTML: OfficialSourceFormat.HTML,
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML: OfficialSourceFormat.HTML,
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_PDF: OfficialSourceFormat.PDF,
    }[role]
    if snapshot.request.source_format is not expected_format:
        raise ValueError(
            "Eurostat construction output snapshot format differs from artifact role"
        )
    if snapshot.status_code != 200:
        raise ValueError("Eurostat construction output snapshot status differs")
    if role is EurostatConstructionOutputArtifactRole.SEARCH_INDEX:
        if (
            snapshot.request.page_number is None
            or snapshot.request.uri != _search_uri(snapshot.request.page_number)
        ):
            raise ValueError(
                "Eurostat construction output search request identity differs"
            )
        try:
            root = safe_xml_fromstring(snapshot.content)
        except (DefusedXmlException, ParseError) as exc:
            raise ValueError(
                "Eurostat construction output search artifact is invalid XML"
            ) from exc
        if root.tag != f"{{{_ATOM_NAMESPACE}}}feed":
            raise ValueError(
                "Eurostat construction output search artifact is not Atom"
            )
    elif role is EurostatConstructionOutputArtifactRole.CURRENT_DATASET:
        if (
            snapshot.request.page_number is not None
            or snapshot.request.uri != EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_URI
        ):
            raise ValueError(
                "Eurostat construction output dataset request identity differs"
            )
        if not snapshot.content.lstrip().startswith(b"{"):
            raise ValueError(
                "Eurostat construction output dataset artifact is not JSON"
            )
    elif snapshot.request.page_number is not None:
        raise ValueError(
            "Eurostat construction output release request has a page number"
        )
    elif role in {
        EurostatConstructionOutputArtifactRole.RELEASE_HTML,
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML,
    }:
        if b"<html" not in snapshot.content[:16_384].lower():
            raise ValueError(
                "Eurostat construction output HTML signature differs"
            )
    elif not snapshot.content.startswith(b"%PDF-"):
        raise ValueError("Eurostat construction output PDF signature differs")
    if role in {
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML,
        EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_PDF,
    } and (
        product_code is None
        or _product_code(snapshot.request.uri) != _product_code(product_code)
    ):
        raise ValueError(
            "Eurostat construction output document product code differs"
        )
    return EurostatConstructionOutputArtifactV1(
        role=role,
        request_uri=snapshot.request.uri,
        resolved_uri=snapshot.resolved_uri,
        source_format=expected_format,
        content_length=len(snapshot.content),
        content_sha256=snapshot.content_sha256,
        page_number=(
            snapshot.request.page_number
            if role is EurostatConstructionOutputArtifactRole.SEARCH_INDEX
            else None
        ),
        product_code=product_code,
    )


def _atom_text(entry: Element, tag: str) -> str:
    node = entry.find(f"{{{_ATOM_NAMESPACE}}}{tag}")
    if node is None or node.text is None:
        raise ValueError(f"Eurostat construction output Atom entry omits {tag}")
    return html.unescape(node.text)


def _atom_link(entry: Element) -> str:
    links = tuple(
        item
        for item in entry.findall(f"{{{_ATOM_NAMESPACE}}}link")
        if item.attrib.get("rel") == "alternate"
    )
    if len(links) != 1 or "href" not in links[0].attrib:
        raise ValueError(
            "Eurostat construction output Atom entry alternate link differs"
        )
    return _https_uri(links[0].attrib["href"], "source_uri")


def parse_eurostat_construction_output_inventory(
    snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatConstructionOutputInventoryV1:
    """Parse and prove the complete bounded construction output search selection."""
    as_of = _iso_date(as_of_date, "as_of_date")
    values = tuple(snapshots)
    if len(values) != EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT:
        raise ValueError(
            "Eurostat construction output inventory snapshot count differs"
        )
    if [item.request.page_number for item in values] != list(
        range(1, EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT + 1)
    ):
        raise ValueError(
            "Eurostat construction output search snapshots are not contiguous"
        )
    artifacts: list[EurostatConstructionOutputArtifactV1] = []
    raw_entries: list[tuple[int, str, str, str, str, str]] = []
    for snapshot in values:
        artifact = _artifact_from_snapshot(
            snapshot, EurostatConstructionOutputArtifactRole.SEARCH_INDEX
        )
        artifacts.append(artifact)
        try:
            root = safe_xml_fromstring(snapshot.content)
        except (DefusedXmlException, ParseError) as exc:
            raise ValueError(
                "Eurostat construction output search page is invalid XML"
            ) from exc
        entries = tuple(root.findall(f"{{{_ATOM_NAMESPACE}}}entry"))
        expected_count = 100
        if (
            artifact.page_number
            == EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT
        ):
            expected_count = (
                EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_RESULT_COUNT
                - (100 * (EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT - 1))
            )
        if len(entries) != expected_count:
            raise ValueError(
                "Eurostat construction output search page size differs"
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
    if len(raw_entries) != EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_RESULT_COUNT:
        raise ValueError(
            "Eurostat construction output search result count differs"
        )
    unique_by_uri: dict[str, tuple[int, str, str, str, str, str]] = {}
    for item in raw_entries:
        previous = unique_by_uri.get(item[2])
        if previous is not None and previous[1:] != item[1:]:
            raise ValueError(
                "Eurostat construction output search duplicate URI differs"
            )
        unique_by_uri.setdefault(item[2], item)
    if (
        len(unique_by_uri)
        != EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_UNIQUE_URI_COUNT
    ):
        raise ValueError(
            "Eurostat construction output search unique URI count differs"
        )
    unique_entries = tuple(unique_by_uri.values())
    title_match_count = sum(
        re.search(r"\bconstruction\b", title, re.IGNORECASE) is not None
        for _, title, _, _, _, _ in unique_entries
    )
    prefiltered = [
        item for item in unique_entries if _inventory_prefilter(item[1])
    ]
    selected: list[EurostatConstructionOutputInventoryEntryV1] = []
    exclusions: list[EurostatConstructionOutputIndexExclusionV1] = []
    for page, title, uri, published, updated, summary in unique_entries:
        product_code = _product_code(uri)
        release_date = _product_release_date(product_code)
        if release_date > as_of:
            continue
        if not _inventory_selected(title, product_code):
            exclusions.append(
                EurostatConstructionOutputIndexExclusionV1(
                    product_code=product_code,
                    release_date=release_date,
                    source_title=title,
                    source_uri=uri,
                    reason=_exclusion_reason(title),
                )
            )
            continue
        selected.append(
            EurostatConstructionOutputInventoryEntryV1(
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
    return EurostatConstructionOutputInventoryV1(
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


def parse_eurostat_construction_output_dataset(
    snapshot: OfficialRawSnapshotV1,
) -> EurostatConstructionOutputDatasetV1:
    """Decode the exact current-revised construction output JSON-stat cross-check."""
    artifact = _artifact_from_snapshot(
        snapshot, EurostatConstructionOutputArtifactRole.CURRENT_DATASET
    )
    try:
        payload = _mapping(json.loads(snapshot.content), "dataset")
        lexical_payload = _mapping(
            json.loads(snapshot.content, parse_float=str), "lexical dataset"
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "Eurostat construction output dataset is invalid JSON"
        ) from exc
    identifiers = [str(item) for item in _sequence(payload.get("id"), "id")]
    expected_ids = [
        "freq",
        "indic_bt",
        "nace_r2",
        "s_adj",
        "unit",
        "geo",
        "time",
    ]
    if identifiers != expected_ids:
        raise ValueError(
            "Eurostat construction output dataset dimensions differ"
        )
    sizes = tuple(_sequence(payload.get("size"), "size"))
    if any(
        isinstance(item, bool) or not isinstance(item, int) for item in sizes
    ):
        raise TypeError(
            "Eurostat construction output dimension sizes must be integers"
        )
    dimension = _mapping(payload.get("dimension"), "dimension")
    expected_codes = {
        "freq": ("M",),
        "indic_bt": ("PRD",),
        "nace_r2": ("F",),
        "s_adj": ("CA", "SCA"),
        "unit": ("PCH_PRE", "PCH_SM"),
        "geo": _ECONOMIES,
    }
    observed_codes = {
        name: _dimension_codes(_mapping(dimension[name], name), name)
        for name in expected_codes
    }
    if observed_codes != expected_codes:
        raise ValueError(
            "Eurostat construction output dataset dimensions or codes differ"
        )
    time_codes = _dimension_codes(_mapping(dimension["time"], "time"), "time")
    if not time_codes or any(
        _MONTH_RE.fullmatch(item) is None for item in time_codes
    ):
        raise ValueError(
            "Eurostat construction output dataset time codes differ"
        )
    if time_codes != _month_range(time_codes[0], time_codes[-1]):
        raise ValueError(
            "Eurostat construction output dataset time codes are not contiguous"
        )
    expected_sizes = (1, 1, 1, 2, 2, len(_ECONOMIES), len(time_codes))
    if tuple(cast(Sequence[int], sizes)) != expected_sizes:
        raise ValueError("Eurostat construction output dataset sizes differ")
    values = _mapping(payload.get("value"), "value")
    lexical_values = _mapping(lexical_payload.get("value"), "lexical value")
    statuses = _mapping(payload.get("status", {}), "status")
    if set(values) != set(lexical_values):
        raise ValueError(
            "Eurostat construction output lexical and numeric values differ"
        )
    if not set(statuses) <= set(values):
        raise ValueError(
            "Eurostat construction output status has no observation"
        )
    observations: list[EurostatConstructionOutputObservationV1] = []
    time_count = len(time_codes)
    geo_count = len(_ECONOMIES)
    maximum = 2 * 2 * geo_count * time_count
    for raw_index, raw_value in values.items():
        try:
            flat_index = int(str(raw_index))
        except ValueError as exc:
            raise ValueError(
                "Eurostat construction output sparse index is invalid"
            ) from exc
        if not 0 <= flat_index < maximum:
            raise ValueError(
                "Eurostat construction output sparse index is outside its cube"
            )
        s_adj_index, remainder = divmod(flat_index, 2 * geo_count * time_count)
        unit_index, remainder = divmod(remainder, geo_count * time_count)
        geo_index, time_index = divmod(remainder, time_count)
        measure = {
            (1, 0): EurostatConstructionOutputMeasure.MONTH_OVER_MONTH,
            (0, 1): EurostatConstructionOutputMeasure.YEAR_OVER_YEAR,
        }.get((s_adj_index, unit_index))
        if measure is None:
            raise ValueError(
                "Eurostat construction output dataset populated an unsupported "
                "adjustment and unit combination"
            )
        lexical = str(lexical_values[raw_index])
        observations.append(
            EurostatConstructionOutputObservationV1(
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
            list(EurostatConstructionOutputMeasure).index(item.measure),
            _ECONOMY_ORDER[item.economy_code],
            item.reference_period,
        )
    )
    return EurostatConstructionOutputDatasetV1(
        label=str(payload.get("label", "")),
        updated_at=str(payload.get("updated", "")),
        time_start=time_codes[0],
        time_end=time_codes[-1],
        artifact=artifact,
        observations=tuple(observations),
    )


class EurostatConstructionOutputMovement(str, Enum):
    """Direction wording attached to the source-authored headline rate."""

    REPORTED = "reported"
    UP = "up"
    DOWN = "down"
    STABLE = "stable"

    @classmethod
    def from_value(
        cls, value: str | EurostatConstructionOutputMovement
    ) -> EurostatConstructionOutputMovement:
        try:
            return value if isinstance(value, cls) else cls(value)
        except ValueError as exc:
            raise ValueError(
                "unsupported Eurostat construction output movement"
            ) from exc


@dataclass(frozen=True, slots=True)
class EurostatConstructionOutputReleaseValueV1:
    """Source-era euro-area construction output rate in a release headline."""

    economy_code: str
    reference_period: str
    measure: EurostatConstructionOutputMeasure
    movement: EurostatConstructionOutputMovement
    value_lexical: str
    value: float
    evidence_id: str
    release_value_id: str = ""
    schema_version: str = (
        EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_VALUE_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_VALUE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat construction output release-value schema"
            )
        if self.economy_code != "EA":
            raise ValueError(
                "Eurostat construction output release value must retain source-era EA scope"
            )
        reference = _period(self.reference_period, "reference_period")
        measure = EurostatConstructionOutputMeasure.from_value(self.measure)
        expected_measure = (
            EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER
            if _period_frequency(reference) == "quarterly"
            else EurostatConstructionOutputMeasure.MONTH_OVER_MONTH
        )
        if measure is not expected_measure:
            raise ValueError(
                "Eurostat construction output headline measure and frequency differ"
            )
        movement = EurostatConstructionOutputMovement.from_value(self.movement)
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _NUMBER_RE.fullmatch(lexical) is None or lexical.startswith("-"):
            raise ValueError(
                "Eurostat construction output headline rate is invalid"
            )
        magnitude = float(lexical)
        numeric = {
            EurostatConstructionOutputMovement.DOWN: -magnitude,
            EurostatConstructionOutputMovement.STABLE: 0.0,
        }.get(movement, magnitude)
        if self.value != numeric or not math.isfinite(self.value):
            raise ValueError(
                "Eurostat construction output headline numeric value differs"
            )
        if not -250.0 <= self.value <= 250.0:
            raise ValueError(
                "Eurostat construction output headline value is outside its bound"
            )
        evidence_id = _required_text(self.evidence_id, "evidence_id")
        if not evidence_id.startswith(
            "eurostat-construction-output-inventory-entry:sha256:"
        ):
            raise ValueError(
                "Eurostat construction output headline evidence differs"
            )
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "measure", measure)
        object.__setattr__(self, "movement", movement)
        object.__setattr__(self, "value_lexical", lexical)
        object.__setattr__(self, "evidence_id", evidence_id)
        expected = _stable_id(
            "eurostat-construction-output-release-value",
            self.identity_payload(),
        )
        if self.release_value_id and self.release_value_id != expected:
            raise ValueError(
                "Eurostat construction output release-value identity differs"
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
    ) -> EurostatConstructionOutputReleaseValueV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatConstructionOutputMeasure.from_value(
                str(data.get("measure", ""))
            ),
            movement=EurostatConstructionOutputMovement.from_value(
                str(data.get("movement", ""))
            ),
            value_lexical=str(data.get("value_lexical", "")),
            value=float(cast(float, data.get("value"))),
            evidence_id=str(data.get("evidence_id", "")),
            release_value_id=str(data.get("release_value_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EurostatConstructionOutputPublishedValueV1:
    """One source-era value read from a release publication table."""

    economy_code: str
    reference_period: str
    measure: EurostatConstructionOutputMeasure
    value_lexical: str
    value: float
    evidence_artifact_sha256: str
    evidence_locator: str
    published_value_id: str = ""
    schema_version: str = (
        EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUE_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat construction output published-value schema"
            )
        economy = _required_text(self.economy_code, "economy_code")
        if economy not in _RELEASE_ECONOMIES:
            raise ValueError(
                "Eurostat construction output published-value economy differs"
            )
        reference = _period(self.reference_period, "reference_period")
        measure = EurostatConstructionOutputMeasure.from_value(self.measure)
        if (
            measure is EurostatConstructionOutputMeasure.MONTH_OVER_MONTH
            and _period_frequency(reference) != "monthly"
        ) or (
            measure is EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER
            and _period_frequency(reference) != "quarterly"
        ):
            raise ValueError(
                "Eurostat construction output published measure and frequency differ"
            )
        lexical = _required_text(self.value_lexical, "value_lexical")
        if _SIGNED_NUMBER_RE.fullmatch(lexical) is None:
            raise ValueError(
                "Eurostat construction output published-value lexical is invalid"
            )
        numeric = float(lexical)
        if numeric != self.value or not math.isfinite(numeric):
            raise ValueError(
                "Eurostat construction output published-value numeric differs"
            )
        if not -250.0 <= numeric <= 250.0:
            raise ValueError(
                "Eurostat construction output published value is outside its bound"
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
            "eurostat-construction-output-published-value",
            self.identity_payload(),
        )
        if self.published_value_id and self.published_value_id != expected:
            raise ValueError(
                "Eurostat construction output published-value identity differs"
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
    ) -> EurostatConstructionOutputPublishedValueV1:
        return cls(
            economy_code=str(data.get("economy_code", "")),
            reference_period=str(data.get("reference_period", "")),
            measure=EurostatConstructionOutputMeasure.from_value(
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


def _ea_composition_for_period(reference_period: str) -> str:
    end = _period_end(reference_period)
    reference = f"{end.year:04d}-{end.month:02d}"
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
    reference = _period(reference_period, "reference_period")
    lag = (released - _period_end(reference)).days
    bounds = (
        (19, 70) if _period_frequency(reference) == "monthly" else (40, 110)
    )
    if not bounds[0] <= lag <= bounds[1]:
        raise ValueError(
            "Eurostat construction output release lag is outside the qualified window"
        )
    return lag


_HEADLINE_VALUE_RE = re.compile(
    r"\b(?:production in (?:the )?construction(?: sector)?|construction output)\b"
    r".*?(?:(?P<movement>up|down)\s+(?:by\s+)?"
    r"(?P<value>\d+(?:\.\d+)?)\s*%|(?P<stable>stable|unchanged|steady)\b)",
    re.IGNORECASE,
)


def _headline_value(
    entry: EurostatConstructionOutputInventoryEntryV1,
    reference_period: str,
) -> EurostatConstructionOutputReleaseValueV1:
    match = _HEADLINE_VALUE_RE.search(entry.source_title)
    if match is None:
        raise ValueError(
            "Eurostat construction output release headline has no rate"
        )
    movement_token = (
        match.group("movement") or match.group("stable") or ""
    ).casefold()
    movement = (
        EurostatConstructionOutputMovement.STABLE
        if movement_token in {"stable", "unchanged", "steady"}
        else EurostatConstructionOutputMovement.from_value(movement_token)
    )
    lexical = match.group("value") or "0"
    magnitude = float(lexical)
    numeric = {
        EurostatConstructionOutputMovement.DOWN: -magnitude,
        EurostatConstructionOutputMovement.STABLE: 0.0,
    }.get(movement, magnitude)
    return EurostatConstructionOutputReleaseValueV1(
        economy_code="EA",
        reference_period=reference_period,
        measure=(
            EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER
            if _period_frequency(reference_period) == "quarterly"
            else EurostatConstructionOutputMeasure.MONTH_OVER_MONTH
        ),
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
    entry: EurostatConstructionOutputInventoryEntryV1,
) -> tuple[str, str, str | None, tuple[str, ...]]:
    try:
        content = snapshot.content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "Eurostat construction output release landing is not UTF-8"
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
            "Eurostat construction output Atom and landing titles differ"
        )
    text = " ".join(parser.text)
    page_dates: set[str] = set()
    for date_match in _LANDING_DATE_RE.finditer(text):
        month = _MONTHS.get(date_match.group("month").casefold())
        if month is None:
            raise ValueError(
                "Eurostat construction output landing month is unsupported"
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
                "Eurostat construction output landing date is invalid"
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
            "Eurostat construction output release landing has no unique publication time"
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
            "Eurostat construction output release landing has no unique release date"
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
            "Eurostat construction output landing exposes multiple English release documents"
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
                    "Eurostat construction output release PDF has too many unreadable pages"
                )
            text = " ".join(page_text)
        except (PdfReadError, ValueError) as exc:
            raise ValueError(
                "Eurostat construction output release PDF text extraction failed"
            ) from exc
    else:
        raise ValueError(
            "Eurostat construction output release value source format differs"
        )
    normalized = _SPACE_RE.sub(" ", text.replace("\u2212", "-")).strip()
    if not normalized:
        raise ValueError(
            "Eurostat construction output release value source has no text"
        )
    return normalized


_REFERENCE_MONTH_RE = re.compile(
    r"\b(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(?P<year>20\d{2})\b",
    re.IGNORECASE,
)
_REFERENCE_MONTH_COMPARISON_RE = re.compile(
    r"\b(?P<month>January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+(?P<year>20\d{2})\s+"
    r"compared\s+with\b",
    re.IGNORECASE,
)
_REFERENCE_QUARTER_RE = re.compile(
    r"\b(?P<quarter>first|second|third|fourth|1st|2nd|3rd|4th)\s+"
    r"quarter(?:\s+of)?\s+(?P<year>20\d{2})\b",
    re.IGNORECASE,
)
_QUARTER_NUMBER: Final = {
    "first": 1,
    "1st": 1,
    "second": 2,
    "2nd": 2,
    "third": 3,
    "3rd": 3,
    "fourth": 4,
    "4th": 4,
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


def _reference_period_from_publication(
    entry: EurostatConstructionOutputInventoryEntryV1,
    landing_snapshot: OfficialRawSnapshotV1,
    document_snapshot: OfficialRawSnapshotV1 | None,
) -> tuple[str, int]:
    # Exact publication evidence outranks the search-feed fallback. In
    # particular, 4-19052016-ap has an unrelated inflation Atom summary.
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
        if (
            entry.release_date
            < EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_MONTHLY_RELEASE_DATE
        ):
            for match in _REFERENCE_QUARTER_RE.finditer(text):
                quarter = _QUARTER_NUMBER[match.group("quarter").casefold()]
                reference_period = f"{match.group('year')}-Q{quarter}"
                try:
                    lag = _release_lag(entry.release_date, reference_period)
                except ValueError:
                    continue
                return reference_period, lag
            continue
        month_matches = chain(
            _REFERENCE_MONTH_COMPARISON_RE.finditer(text),
            _REFERENCE_MONTH_RE.finditer(text),
        )
        for match in month_matches:
            month = _MONTHS[match.group("month").casefold()]
            reference_period = f"{match.group('year')}-{month:02d}"
            try:
                lag = _release_lag(entry.release_date, reference_period)
            except ValueError:
                continue
            return reference_period, lag
    inferred = EUROSTAT_CONSTRUCTION_OUTPUT_REFERENCE_PERIOD_INFERENCES.get(
        entry.product_code
    )
    if inferred is not None:
        return inferred, _release_lag(entry.release_date, inferred)
    raise ValueError(
        "Eurostat construction output publication omits a qualified reference "
        f"period for {entry.product_code}"
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
            "Eurostat construction output source-stated EA composition differs: "
            f"expected={expected}, observed={tuple(sorted(labels))}"
        )
    return expected, expected in labels


class _ConstructionOutputTableParser(HTMLParser):
    """Retain source-authored HTML table cells without layout styling."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tables: list[tuple[tuple[str, ...], ...]] = []
        self.table_contexts: list[str] = []
        self.table_measures: list[EurostatConstructionOutputMeasure | None] = []
        self._table: list[tuple[str, ...]] | None = None
        self._table_context = ""
        self._active_measure: EurostatConstructionOutputMeasure | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self._outside_text: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
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

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)
        elif self._table is None and not self._ignored_depth:
            value = _SPACE_RE.sub(" ", data).strip()
            if value:
                self._outside_text.append(value)
                normalized = value.casefold()
                if (
                    "monthly variation" in normalized
                    or "change compared with previous month" in normalized
                    or "monthly comparison" in normalized
                ):
                    self._active_measure = (
                        EurostatConstructionOutputMeasure.MONTH_OVER_MONTH
                    )
                elif (
                    "quarterly variation" in normalized
                    or "change compared with previous quarter" in normalized
                    or "quarterly comparison" in normalized
                ):
                    self._active_measure = (
                        EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER
                    )
                elif (
                    "annual variation" in normalized
                    or "same month of the previous year" in normalized
                    or "same quarter of the previous year" in normalized
                    or "annual comparison" in normalized
                ):
                    self._active_measure = (
                        EurostatConstructionOutputMeasure.YEAR_OVER_YEAR
                    )
                elif "production indices for total construction" in normalized:
                    self._active_measure = None

    def handle_endtag(self, tag: str) -> None:
        name = tag.casefold()
        if name in {"script", "style", "template", "noscript"}:
            if self._ignored_depth:
                self._ignored_depth -= 1
            return
        if name in {"td", "th"} and self._cell is not None:
            assert self._row is not None
            self._row.append(_SPACE_RE.sub(" ", " ".join(self._cell)).strip())
            self._cell = None
        elif name == "tr" and self._row is not None:
            if any(self._row):
                assert self._table is not None
                self._table.append(tuple(self._row))
            self._row = None
        elif name == "table" and self._table is not None:
            if self._table:
                self.tables.append(tuple(self._table))
                self.table_contexts.append(self._table_context)
                self.table_measures.append(self._active_measure)
            self._table = None
            self._table_context = ""


_CONSTRUCTION_MONTH_NAMES: Final = {
    name.casefold(): number
    for number, name in enumerate(
        (
            "",
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        )
    )
    if name
}
_CONSTRUCTION_PERIOD_CELL_RE = re.compile(
    r"^(?:(?P<name>[A-Za-z]{3,9})[-/ ](?P<name_year>\d{2,4})|"
    r"(?P<number>0?[1-9]|1[0-2])/(?P<number_year>\d{2,4})|"
    r"(?P<year>20\d{2})[-/](?P<year_month>0?[1-9]|1[0-2]))$"
)
_CONSTRUCTION_QUARTER_CELL_RE = re.compile(
    r"^(?:(?P<year_first>20\d{2})[-/ ]?Q(?P<quarter_first>[1-4])|"
    r"Q(?P<quarter_second>[1-4])[-/ ]?(?P<year_second>\d{2,4})|"
    r"(?P<quarter_ordinal>[1-4])(?:st|nd|rd|th)?\s+quarter(?:\s+of)?\s+"
    r"(?P<year_ordinal>\d{2,4}))$",
    re.IGNORECASE,
)
_CONSTRUCTION_VALUE_CELL_RE = re.compile(
    r"^[+\-\u2212]?(?:\d+(?:\.\d+)?|\.\d+)"
)
_CONSTRUCTION_MONTHLY_MARKERS: Final = (
    "previous month",
    "monthly comparison",
    "month-on-month",
    "month on month",
)
_CONSTRUCTION_QUARTERLY_MARKERS: Final = (
    "previous quarter",
    "quarterly comparison",
    "quarter-on-quarter",
    "quarter on quarter",
)
_CONSTRUCTION_ANNUAL_MARKERS: Final = (
    "same month one year ago",
    "same month of the previous year",
    "same quarter one year ago",
    "same quarter of the previous year",
    "annual comparison",
    "year-on-year",
    "year on year",
)
_CONSTRUCTION_PERIOD_TOKEN_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:"
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
    r"Dec(?:ember)?)[-/ ]\d{2,4}|"
    r"(?:0?[1-9]|1[0-2])/\d{2,4}|20\d{2}[-/](?:0?[1-9]|1[0-2])|"
    r"20\d{2}[-/ ]?Q[1-4]|Q[1-4][-/ ]?\d{2,4}|"
    r"[1-4](?:st|nd|rd|th)?\s+quarter(?:\s+of)?\s+\d{2,4})"
    r"(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_CONSTRUCTION_PDF_ROW_RE = re.compile(
    rf"^\s*(?P<label>EA{_EA_COMPOSITION_NUMBER_PATTERN}(?:[1-9]\d?)?|"
    rf"Euro[ -](?:area|zone)(?:\s*(?:\((?:EA)?"
    rf"{_EA_COMPOSITION_NUMBER_PATTERN}\)|EA"
    rf"{_EA_COMPOSITION_NUMBER_PATTERN}|"
    rf"{_EA_COMPOSITION_NUMBER_PATTERN}(?=\s+[+\-−]?\d)))?|"
    r"Germany|DE|France|FR)\b(?P<body>.*)$",
    re.IGNORECASE,
)
_CONSTRUCTION_PDF_NUMBER_RE = re.compile(
    r"(?<![A-Za-z0-9.])[+\-−]?(?:\d+(?:\.\d+)?|\.\d+)(?![A-Za-z0-9.])|:"
)
_CONSTRUCTION_PDF_SECTION_RE = re.compile(
    r"\b(?:(?P<total>Total construction)|"
    r"(?P<component>Buildings?|Building construction|Civil engineering))\b",
    re.IGNORECASE,
)


def _construction_period_cell(value: str) -> str | None:
    token = _SPACE_RE.sub(" ", value).strip(" *\u00a0")
    quarter_match = _CONSTRUCTION_QUARTER_CELL_RE.fullmatch(token)
    if quarter_match is not None:
        quarter = int(
            cast(
                str,
                quarter_match.group("quarter_first")
                or quarter_match.group("quarter_second")
                or quarter_match.group("quarter_ordinal"),
            )
        )
        raw_year = cast(
            str,
            quarter_match.group("year_first")
            or quarter_match.group("year_second")
            or quarter_match.group("year_ordinal"),
        )
        year = int(raw_year) + (2000 if len(raw_year) == 2 else 0)
        return f"{year:04d}-Q{quarter}"
    match = _CONSTRUCTION_PERIOD_CELL_RE.fullmatch(token)
    if match is None:
        return None
    if match.group("name"):
        month = _CONSTRUCTION_MONTH_NAMES.get(
            match.group("name")[:3].casefold()
        )
        raw_year = cast(str, match.group("name_year"))
    elif match.group("number"):
        month = int(cast(str, match.group("number")))
        raw_year = cast(str, match.group("number_year"))
    else:
        month = int(cast(str, match.group("year_month")))
        raw_year = cast(str, match.group("year"))
    if month is None:
        return None
    year = int(raw_year) + (2000 if len(raw_year) == 2 else 0)
    return f"{year:04d}-{month:02d}"


def _construction_table_measure(
    rows: Sequence[Sequence[str]],
) -> EurostatConstructionOutputMeasure | None:
    text = _SPACE_RE.sub(
        " ", " ".join(cell for row in rows[:4] for cell in row if cell)
    ).casefold()
    text = re.sub(r"\bye\s+ar\b", "year", text)
    if any(marker in text for marker in _CONSTRUCTION_ANNUAL_MARKERS):
        return EurostatConstructionOutputMeasure.YEAR_OVER_YEAR
    if any(marker in text for marker in _CONSTRUCTION_QUARTERLY_MARKERS):
        return EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER
    if any(marker in text for marker in _CONSTRUCTION_MONTHLY_MARKERS):
        return EurostatConstructionOutputMeasure.MONTH_OVER_MONTH
    return None


def _construction_economy_cell(
    value: str, *, expected_ea_composition: str
) -> str | None:
    token = re.sub(r"[²³*]+", "", _SPACE_RE.sub(" ", value)).strip().casefold()
    if re.fullmatch(
        rf"(?:euro[ -](?:area|zone)(?:\s*\(?(?:ea)?"
        rf"{_EA_COMPOSITION_NUMBER_PATTERN}\)?)?|"
        rf"ea{_EA_COMPOSITION_NUMBER_PATTERN}(?:[1-9]\d?)?)",
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


def _construction_value_cell(value: str) -> str | None:
    token = _SPACE_RE.sub(" ", value).strip()
    if token in {"", ":", "-", ".."}:
        return None
    match = _CONSTRUCTION_VALUE_CELL_RE.match(token)
    if match is None:
        return None
    return match.group(0).replace("\u2212", "-")


def _construction_html_table_values(
    snapshot: OfficialRawSnapshotV1,
    artifact: EurostatConstructionOutputArtifactV1,
    reference_period: str,
) -> tuple[EurostatConstructionOutputPublishedValueV1, ...]:
    if snapshot.request.source_format is not OfficialSourceFormat.HTML:
        return ()
    try:
        source = snapshot.content.decode("utf-8")
    except UnicodeDecodeError:
        source = snapshot.content.decode("cp1252")
    parser = _ConstructionOutputTableParser()
    parser.feed(source)
    parser.close()
    expected_ea_composition = _ea_composition_for_period(reference_period)
    values: dict[
        tuple[str, str, EurostatConstructionOutputMeasure],
        EurostatConstructionOutputPublishedValueV1,
    ] = {}
    for table_number, (context, declared_measure, rows) in enumerate(
        zip(
            parser.table_contexts,
            parser.table_measures,
            parser.tables,
            strict=True,
        ),
        start=1,
    ):
        del context
        table_header = " ".join(
            cell.casefold() for row in rows[:4] for cell in row if cell
        )
        if "indices" in table_header and "construction" in table_header:
            continue
        if any(
            component in table_header
            for component in ("building construction", "civil engineering")
        ):
            continue
        leading_label = _SPACE_RE.sub(" ", rows[0][0]).strip().casefold()
        if re.fullmatch(
            r"(?:buildings?|building construction|civil engineering)",
            leading_label,
        ):
            continue
        measure = _construction_table_measure(rows) or declared_measure
        if measure is None:
            continue
        if measure in {
            EurostatConstructionOutputMeasure.MONTH_OVER_MONTH,
            EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER,
        }:
            measure = (
                EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER
                if _period_frequency(reference_period) == "quarterly"
                else EurostatConstructionOutputMeasure.MONTH_OVER_MONTH
            )
        header_index = -1
        period_columns: dict[int, str] = {}
        for row_index, row in enumerate(rows):
            candidates = {
                column: period
                for column, cell in enumerate(row)
                if (period := _construction_period_cell(cell)) is not None
            }
            if len(candidates) > len(period_columns):
                header_index = row_index
                period_columns = candidates
        if reference_period not in period_columns.values():
            continue
        minimum_period = _period_shift(
            reference_period,
            -4 if _period_frequency(reference_period) == "quarterly" else -12,
        )
        for row in rows[header_index + 1 :]:
            economy = next(
                (
                    candidate
                    for cell in row[:3]
                    if (
                        candidate := _construction_economy_cell(
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
            for column, period in period_columns.items():
                if (
                    _period_frequency(period)
                    != _period_frequency(reference_period)
                    or not minimum_period <= period <= reference_period
                    or column >= len(row)
                ):
                    continue
                lexical = _construction_value_cell(row[column])
                if lexical is None:
                    continue
                item = EurostatConstructionOutputPublishedValueV1(
                    economy_code=economy,
                    reference_period=period,
                    measure=measure,
                    value_lexical=lexical,
                    value=float(lexical),
                    evidence_artifact_sha256=artifact.content_sha256,
                    evidence_locator=(
                        f"construction-output HTML table {table_number}; "
                        f"{economy} row; {period}; {measure.value}"
                    ),
                )
                previous = values.get((economy, period, measure))
                if previous is not None and previous.value != item.value:
                    raise ValueError(
                        "Eurostat construction output HTML tables conflict for "
                        f"{artifact.product_code}: {economy} {period} {measure.value}"
                    )
                values.setdefault((economy, period, measure), item)
    return tuple(
        sorted(
            values.values(),
            key=lambda item: (
                item.reference_period,
                _RELEASE_ECONOMY_ORDER[item.economy_code],
                list(EurostatConstructionOutputMeasure).index(item.measure),
            ),
        )
    )


def _construction_pdf_page_values(
    page_text: str,
    artifact: EurostatConstructionOutputArtifactV1,
    reference_period: str,
    page_number: int,
) -> tuple[EurostatConstructionOutputPublishedValueV1, ...]:
    """Decode source rows from one text-bearing release PDF page."""
    normalized = page_text.replace("\u2212", "-")
    normalized = re.sub(r"(?<=\d)\.\s+(?=\d(?:\s|$))", ".", normalized)
    normalized = re.sub(r"(?<!\w)-\s+(?=\d)", "-", normalized)
    lines = tuple(
        line.strip() for line in normalized.splitlines() if line.strip()
    )
    measure = _construction_table_measure((lines[:8],))
    if measure is None:
        return ()
    if measure in {
        EurostatConstructionOutputMeasure.MONTH_OVER_MONTH,
        EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER,
    }:
        measure = (
            EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER
            if _period_frequency(reference_period) == "quarterly"
            else EurostatConstructionOutputMeasure.MONTH_OVER_MONTH
        )
    period_columns: tuple[str, ...] = ()
    pending_economy: str | None = None
    active_total_section: bool | None = None
    values: dict[
        tuple[str, str, EurostatConstructionOutputMeasure],
        EurostatConstructionOutputPublishedValueV1,
    ] = {}
    minimum_period = _period_shift(
        reference_period,
        -4 if _period_frequency(reference_period) == "quarterly" else -12,
    )
    expected_ea_composition = _ea_composition_for_period(reference_period)
    for line in lines:
        section_match = _CONSTRUCTION_PDF_SECTION_RE.search(line)
        if section_match is not None:
            active_total_section = section_match.group("total") is not None
        periods = tuple(
            period
            for match in _CONSTRUCTION_PERIOD_TOKEN_RE.finditer(line)
            if (period := _construction_period_cell(match.group(0))) is not None
        )
        correction = EUROSTAT_CONSTRUCTION_OUTPUT_PERIOD_HEADER_CORRECTIONS.get(
            cast(str, artifact.product_code)
        )
        if correction is not None and periods == correction[0]:
            periods = correction[1]
        if len(periods) >= 3 and reference_period in periods:
            if active_total_section is False:
                period_columns = ()
                pending_economy = None
                continue
            if active_total_section is None:
                active_total_section = True
            period_columns = periods
            pending_economy = None
            continue
        if not period_columns:
            continue
        row_match = _CONSTRUCTION_PDF_ROW_RE.match(line)
        if row_match is not None:
            economy = _construction_economy_cell(
                row_match.group("label"),
                expected_ea_composition=expected_ea_composition,
            )
            body = row_match.group("body").strip()
            if economy is None:
                pending_economy = None
                continue
            if not body:
                pending_economy = economy
                continue
        elif pending_economy is not None and re.match(
            r"^Total construction\d*\b", line, re.IGNORECASE
        ):
            economy = pending_economy
            body = line
        else:
            continue
        pending_economy = None
        body = re.sub(
            r"^Total construction\d*\s*", "", body, flags=re.IGNORECASE
        )
        tokens = tuple(
            match.group(0).replace("\u2212", "-")
            for match in _CONSTRUCTION_PDF_NUMBER_RE.finditer(body)
        )
        if len(tokens) != len(period_columns):
            continue
        for period, lexical in zip(period_columns, tokens, strict=True):
            if (
                lexical == ":"
                or _period_frequency(period)
                != _period_frequency(reference_period)
                or not minimum_period <= period <= reference_period
            ):
                continue
            item = EurostatConstructionOutputPublishedValueV1(
                economy_code=economy,
                reference_period=period,
                measure=measure,
                value_lexical=lexical,
                value=float(lexical),
                evidence_artifact_sha256=artifact.content_sha256,
                evidence_locator=(
                    f"construction-output PDF page {page_number}; "
                    f"{economy} row; {period}; {measure.value}"
                ),
            )
            key = (economy, period, measure)
            previous = values.get(key)
            if previous is not None and previous.value != item.value:
                raise ValueError(
                    "Eurostat construction output PDF tables conflict for "
                    f"{artifact.product_code}: {economy} {period} {measure.value}"
                )
            values.setdefault(key, item)
    return tuple(values.values())


def _construction_pdf_table_values(
    snapshot: OfficialRawSnapshotV1,
    artifact: EurostatConstructionOutputArtifactV1,
    reference_period: str,
) -> tuple[EurostatConstructionOutputPublishedValueV1, ...]:
    if snapshot.request.source_format is not OfficialSourceFormat.PDF:
        return ()
    try:
        reader = PdfReader(BytesIO(snapshot.content), strict=False)
    except PdfReadError as exc:
        raise ValueError(
            "Eurostat construction output release PDF table extraction failed"
        ) from exc
    values: dict[
        tuple[str, str, EurostatConstructionOutputMeasure],
        EurostatConstructionOutputPublishedValueV1,
    ] = {}
    failed_pages: list[int] = []
    readable_page_count = 0
    for page_number, page in enumerate(reader.pages, start=1):
        try:
            page_text = page.extract_text() or ""
        except PdfReadError as exc:
            failed_pages.append(page_number)
            if len(failed_pages) > 1:
                raise ValueError(
                    "Eurostat construction output release PDF table has too many "
                    "unreadable pages"
                ) from exc
            continue
        if page_text.strip():
            readable_page_count += 1
        for item in _construction_pdf_page_values(
            page_text, artifact, reference_period, page_number
        ):
            key = (item.economy_code, item.reference_period, item.measure)
            previous = values.get(key)
            if previous is not None and previous.value != item.value:
                raise ValueError(
                    "Eurostat construction output PDF pages conflict for "
                    f"{artifact.product_code}: {item.economy_code} "
                    f"{item.reference_period} {item.measure.value}"
                )
            values.setdefault(key, item)
    if not readable_page_count:
        raise ValueError(
            "Eurostat construction output release PDF table has no readable text"
        )
    return tuple(
        sorted(
            values.values(),
            key=lambda item: (
                item.reference_period,
                _RELEASE_ECONOMY_ORDER[item.economy_code],
                list(EurostatConstructionOutputMeasure).index(item.measure),
            ),
        )
    )


def _construction_table_published_values(
    snapshot: OfficialRawSnapshotV1,
    artifact: EurostatConstructionOutputArtifactV1,
    reference_period: str,
) -> tuple[EurostatConstructionOutputPublishedValueV1, ...]:
    values = _construction_html_table_values(
        snapshot, artifact, reference_period
    )
    if values:
        return values
    return _construction_pdf_table_values(snapshot, artifact, reference_period)


def _published_values(
    landing_snapshot: OfficialRawSnapshotV1,
    landing_artifact: EurostatConstructionOutputArtifactV1,
    document_snapshot: OfficialRawSnapshotV1 | None,
    document_artifact: EurostatConstructionOutputArtifactV1 | None,
    reference_period: str,
    headline: EurostatConstructionOutputReleaseValueV1,
) -> tuple[EurostatConstructionOutputPublishedValueV1, ...]:
    parsed: tuple[EurostatConstructionOutputPublishedValueV1, ...] = ()
    for snapshot, artifact in (
        (document_snapshot, document_artifact),
        (landing_snapshot, landing_artifact),
    ):
        if snapshot is None or artifact is None:
            continue
        parsed = _construction_table_published_values(
            snapshot, artifact, reference_period
        )
        if parsed:
            break
    values = list(parsed)
    headline_measure = (
        EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER
        if _period_frequency(reference_period) == "quarterly"
        else EurostatConstructionOutputMeasure.MONTH_OVER_MONTH
    )
    headline_match = next(
        (
            item
            for item in values
            if item.economy_code == "EA"
            and item.measure is headline_measure
            and item.reference_period == reference_period
        ),
        None,
    )
    if headline_match is None:
        values.append(
            EurostatConstructionOutputPublishedValueV1(
                economy_code="EA",
                reference_period=reference_period,
                measure=headline_measure,
                value_lexical=(
                    f"-{headline.value_lexical}"
                    if headline.movement
                    is EurostatConstructionOutputMovement.DOWN
                    else headline.value_lexical
                ),
                value=headline.value,
                evidence_artifact_sha256=landing_artifact.content_sha256,
                evidence_locator="source-authored release headline",
            )
        )
    elif headline_match.value != headline.value:
        raise ValueError(
            "Eurostat construction output headline and publication table differ for "
            f"{landing_artifact.product_code}: headline={headline.value}, "
            f"table={headline_match.value}"
        )
    values.sort(
        key=lambda item: (
            _period_sort_key(item.reference_period),
            _RELEASE_ECONOMY_ORDER[item.economy_code],
            list(EurostatConstructionOutputMeasure).index(item.measure),
        )
    )
    return tuple(values)


@dataclass(frozen=True, slots=True)
class EurostatConstructionOutputReleaseV1:
    """One source-dated quarterly or monthly construction-output release."""

    inventory_entry: EurostatConstructionOutputInventoryEntryV1
    reference_period: str
    reference_period_source_stated: bool
    days_after_period_end: int
    euro_area_composition: str
    composition_source_stated: bool
    page_release_date: str
    page_release_date_offset_days: int
    published_at: str | None
    linked_document_uris: tuple[str, ...]
    artifact: EurostatConstructionOutputArtifactV1
    document_artifact: EurostatConstructionOutputArtifactV1 | None
    headline_value: EurostatConstructionOutputReleaseValueV1
    published_values: tuple[EurostatConstructionOutputPublishedValueV1, ...]
    release_id: str = ""
    schema_version: str = EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat construction output release schema"
            )
        reference = _period(self.reference_period, "reference_period")
        if not isinstance(self.reference_period_source_stated, bool):
            raise TypeError("reference_period_source_stated must be boolean")
        inferred_reference = (
            EUROSTAT_CONSTRUCTION_OUTPUT_REFERENCE_PERIOD_INFERENCES.get(
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
                "Eurostat construction output reference-period evidence differs"
            )
        expected_lag = _release_lag(
            self.inventory_entry.release_date, reference
        )
        if self.days_after_period_end != expected_lag:
            raise ValueError(
                "Eurostat construction output release period or lag differs"
            )
        composition = _required_text(
            self.euro_area_composition, "euro_area_composition"
        )
        if composition != _ea_composition_for_period(reference):
            raise ValueError(
                "Eurostat construction output EA composition differs"
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
            EUROSTAT_CONSTRUCTION_OUTPUT_PAGE_DATE_OFFSET_EXCEPTIONS.get(
                self.inventory_entry.product_code
            )
        )
        if self.page_release_date_offset_days != offset or (
            (expected_exception is None and offset not in {-1, 0})
            or (expected_exception is not None and offset != expected_exception)
        ):
            raise ValueError(
                "Eurostat construction output landing date offset differs for "
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
                    "Eurostat construction output publication timestamp date differs"
                )
        links = tuple(
            _https_uri(item, "linked_document_uri")
            for item in self.linked_document_uris
        )
        if links != tuple(sorted(set(links))):
            raise ValueError(
                "Eurostat construction output release document links are not canonical"
            )
        if len(links) > 1:
            raise ValueError(
                "Eurostat construction output release has multiple English documents"
            )
        if any(
            _product_code(item) != self.inventory_entry.product_code
            for item in links
        ):
            raise ValueError(
                "Eurostat construction output release document product code differs"
            )
        if (
            self.artifact.role
            is not EurostatConstructionOutputArtifactRole.RELEASE_HTML
        ):
            raise ValueError(
                "Eurostat construction output release artifact role differs"
            )
        if (
            self.artifact.product_code != self.inventory_entry.product_code
            or self.artifact.request_uri != self.inventory_entry.source_uri
            or _product_code(self.artifact.resolved_uri)
            != self.inventory_entry.product_code
        ):
            raise ValueError(
                "Eurostat construction output release artifact product code differs"
            )
        document = self.document_artifact
        if (document is None) != (not links):
            raise ValueError(
                "Eurostat construction output release document artifact differs"
            )
        if document is not None and (
            document.role
            not in {
                EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML,
                EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_PDF,
            }
            or document.request_uri != links[0]
            or document.product_code != self.inventory_entry.product_code
            or _product_code(document.resolved_uri)
            != self.inventory_entry.product_code
        ):
            raise ValueError(
                "Eurostat construction output release document identity differs"
            )
        value = self.headline_value
        if (
            value.reference_period != reference
            or value.evidence_id != self.inventory_entry.entry_id
        ):
            raise ValueError(
                "Eurostat construction output release headline occurrence differs"
            )
        published_values = tuple(self.published_values)
        if not (
            1
            <= len(published_values)
            <= MAX_EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUES_PER_RELEASE
        ):
            raise ValueError(
                "Eurostat construction output published-value count differs"
            )
        keys = [item.key for item in published_values]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "Eurostat construction output release repeats a published value"
            )
        if list(published_values) != sorted(
            published_values,
            key=lambda item: (
                _period_sort_key(item.reference_period),
                _RELEASE_ECONOMY_ORDER[item.economy_code],
                list(EurostatConstructionOutputMeasure).index(item.measure),
            ),
        ):
            raise ValueError(
                "Eurostat construction output published values are not canonical"
            )
        evidence_digests = {self.artifact.content_sha256}
        if document is not None:
            evidence_digests.add(document.content_sha256)
        minimum_period = _period_shift(
            reference,
            -4 if _period_frequency(reference) == "quarterly" else -12,
        )
        if any(
            _period_frequency(item.reference_period)
            != _period_frequency(reference)
            or item.reference_period > reference
            or item.reference_period < minimum_period
            or item.evidence_artifact_sha256 not in evidence_digests
            for item in published_values
        ):
            raise ValueError(
                "Eurostat construction output published-value evidence differs"
            )
        headline_measure = (
            EurostatConstructionOutputMeasure.QUARTER_OVER_QUARTER
            if _period_frequency(reference) == "quarterly"
            else EurostatConstructionOutputMeasure.MONTH_OVER_MONTH
        )
        headline_published = next(
            (
                item
                for item in published_values
                if item.economy_code == "EA"
                and item.measure is headline_measure
                and item.reference_period == reference
            ),
            None,
        )
        if (
            headline_published is None
            or headline_published.value != value.value
        ):
            raise ValueError(
                "Eurostat construction output headline published value differs"
            )
        object.__setattr__(self, "reference_period", reference)
        object.__setattr__(self, "euro_area_composition", composition)
        object.__setattr__(self, "page_release_date", page_date)
        object.__setattr__(self, "published_at", published_at)
        object.__setattr__(self, "linked_document_uris", links)
        object.__setattr__(self, "published_values", published_values)
        expected = _stable_id(
            "eurostat-construction-output-release", self.identity_payload()
        )
        if self.release_id and self.release_id != expected:
            raise ValueError(
                "Eurostat construction output release identity differs"
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
    ) -> EurostatConstructionOutputReleaseV1:
        return cls(
            inventory_entry=EurostatConstructionOutputInventoryEntryV1.from_dict(
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
            artifact=EurostatConstructionOutputArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            document_artifact=(
                EurostatConstructionOutputArtifactV1.from_dict(
                    _mapping(data.get("document_artifact"), "document_artifact")
                )
                if data.get("document_artifact") is not None
                else None
            ),
            headline_value=EurostatConstructionOutputReleaseValueV1.from_dict(
                _mapping(data.get("headline_value"), "headline_value")
            ),
            published_values=tuple(
                EurostatConstructionOutputPublishedValueV1.from_dict(
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
    releases: Sequence[EurostatConstructionOutputReleaseV1],
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
    releases: Sequence[EurostatConstructionOutputReleaseV1],
    dataset: EurostatConstructionOutputDatasetV1,
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
class EurostatConstructionOutputArchiveManifestV1:
    """Compact, replayable receipt for the official construction output corpus."""

    as_of_date: str
    registry_id: str
    source_id: str
    inventory: EurostatConstructionOutputInventoryV1
    current_dataset: EurostatConstructionOutputDatasetV1
    releases: tuple[EurostatConstructionOutputReleaseV1, ...]
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
    schema_version: str = EUROSTAT_CONSTRUCTION_OUTPUT_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != EUROSTAT_CONSTRUCTION_OUTPUT_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported Eurostat construction output manifest schema"
            )
        as_of = _iso_date(self.as_of_date, "as_of_date")
        if as_of != EUROSTAT_CONSTRUCTION_OUTPUT_AS_OF_DATE:
            raise ValueError(
                "Eurostat construction output archive boundary differs"
            )
        registry_id = _required_text(self.registry_id, "registry_id")
        source_id = _required_text(self.source_id, "source_id")
        if not registry_id.startswith("official-source-registry:sha256:"):
            raise ValueError(
                "Eurostat construction output registry identity is invalid"
            )
        if not source_id.startswith("official-source:sha256:"):
            raise ValueError(
                "Eurostat construction output source identity is invalid"
            )
        releases = tuple(self.releases)
        if len(releases) != EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_COUNT:
            raise ValueError(
                "Eurostat construction output manifest release count differs"
            )
        if list(releases) != sorted(
            releases,
            key=lambda item: (
                item.inventory_entry.release_date,
                item.inventory_entry.product_code,
            ),
        ):
            raise ValueError(
                "Eurostat construction output releases are not canonical"
            )
        if [item.inventory_entry for item in releases] != list(
            self.inventory.entries
        ):
            raise ValueError(
                "Eurostat construction output release and inventory entries differ"
            )
        if any(
            item.headline_value
            != _headline_value(item.inventory_entry, item.reference_period)
            for item in releases
        ):
            raise ValueError(
                "Eurostat construction output release headline evidence differs"
            )
        occurrences = [item.occurrence_key for item in releases]
        if len(occurrences) != len(set(occurrences)):
            raise ValueError(
                "Eurostat construction output releases repeat an occurrence"
            )
        periods = [item.reference_period for item in releases]
        quarterly_periods = tuple(
            item for item in periods if _period_frequency(item) == "quarterly"
        )
        monthly_periods = tuple(
            item for item in periods if _period_frequency(item) == "monthly"
        )
        if (
            len(quarterly_periods)
            != EUROSTAT_CONSTRUCTION_OUTPUT_QUARTERLY_RELEASE_COUNT
            or len(monthly_periods)
            != EUROSTAT_CONSTRUCTION_OUTPUT_MONTHLY_RELEASE_COUNT
        ):
            raise ValueError(
                "Eurostat construction output release-frequency counts differ"
            )
        if (
            min(periods, key=_period_sort_key)
            != EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_REFERENCE_PERIOD
            or max(periods, key=_period_sort_key)
            != EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_REFERENCE_PERIOD
        ):
            raise ValueError(
                "Eurostat construction output reference range differs"
            )
        unique_quarterly_periods = _period_range(
            EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_REFERENCE_PERIOD,
            EUROSTAT_CONSTRUCTION_OUTPUT_LAST_QUARTERLY_REFERENCE_PERIOD,
        )
        repeated_quarter = (
            EUROSTAT_CONSTRUCTION_OUTPUT_REPEATED_REFERENCE_PERIODS[0]
        )
        repeated_quarter_position = unique_quarterly_periods.index(
            repeated_quarter
        )
        expected_quarterly_periods = (
            *unique_quarterly_periods[: repeated_quarter_position + 1],
            repeated_quarter,
            *unique_quarterly_periods[repeated_quarter_position + 1 :],
        )
        if quarterly_periods != expected_quarterly_periods:
            raise ValueError(
                "Eurostat construction output reference-quarter lineage differs"
            )
        unique_monthly_periods = tuple(
            item
            for item in _month_range(
                EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_MONTHLY_REFERENCE_PERIOD,
                EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_REFERENCE_PERIOD,
            )
            if item
            not in EUROSTAT_CONSTRUCTION_OUTPUT_INTERNAL_GAP_REFERENCE_PERIODS
        )
        if monthly_periods != unique_monthly_periods:
            raise ValueError(
                "Eurostat construction output reference-month lineage differs"
            )
        if any(item.inventory_entry.release_date > as_of for item in releases):
            raise ValueError(
                "Eurostat construction output release exceeds archive boundary"
            )
        if self.current_dataset.updated_at[:10] > as_of:
            raise ValueError(
                "Eurostat construction output dataset exceeds archive boundary"
            )
        expected_gaps = (
            *_month_range(
                self.current_dataset.time_start,
                _month_shift(
                    EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_MONTHLY_REFERENCE_PERIOD,
                    -1,
                ),
            ),
            *EUROSTAT_CONSTRUCTION_OUTPUT_INTERNAL_GAP_REFERENCE_PERIODS,
        )
        gaps = tuple(
            _month(item, "searchable_release_gap_reference_period")
            for item in self.searchable_release_gap_reference_periods
        )
        if gaps != expected_gaps:
            raise ValueError(
                "Eurostat construction output searchable-release gap inventory differs"
            )
        for name, maximum in (
            (
                "reference_period_inferred_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES,
            ),
            (
                "composition_source_stated_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES,
            ),
            (
                "page_date_offset_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES,
            ),
            (
                "exact_publication_time_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES,
            ),
            (
                "release_document_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES,
            ),
            (
                "published_value_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES
                * MAX_EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "revision_comparison_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES
                * MAX_EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "changed_revision_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES
                * MAX_EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "current_dataset_comparison_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES
                * MAX_EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUES_PER_RELEASE,
            ),
            (
                "changed_current_dataset_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_RELEASES
                * MAX_EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUES_PER_RELEASE,
            ),
        ):
            _bounded_int(getattr(self, name), name, maximum)
        expected_inferred_references = sum(
            not item.reference_period_source_stated for item in releases
        )
        if self.reference_period_inferred_count != expected_inferred_references:
            raise ValueError(
                "Eurostat construction output inferred-reference count differs"
            )
        if self.composition_source_stated_count != sum(
            item.composition_source_stated for item in releases
        ):
            raise ValueError(
                "Eurostat construction output composition count differs"
            )
        expected_offsets = sum(
            item.page_release_date_offset_days != 0 for item in releases
        )
        if self.page_date_offset_count != expected_offsets:
            raise ValueError(
                "Eurostat construction output page-date offset count differs"
            )
        expected_exact_times = sum(
            item.published_at is not None for item in releases
        )
        if self.exact_publication_time_count != expected_exact_times:
            raise ValueError(
                "Eurostat construction output exact publication-time count differs"
            )
        document_artifacts = tuple(
            item.document_artifact
            for item in releases
            if item.document_artifact is not None
        )
        if (
            len(document_artifacts)
            != EUROSTAT_CONSTRUCTION_OUTPUT_DOCUMENT_COUNT
        ):
            raise ValueError(
                "Eurostat construction output English document count differs"
            )
        if self.release_document_count != len(document_artifacts):
            raise ValueError(
                "Eurostat construction output release-document count differs"
            )
        expected_value_count = sum(
            len(item.published_values) for item in releases
        )
        if self.published_value_count != expected_value_count:
            raise ValueError(
                "Eurostat construction output published-value count differs"
            )
        revision_counts = _published_revision_counts(releases)
        if revision_counts != (
            self.revision_comparison_count,
            self.changed_revision_count,
        ):
            raise ValueError(
                "Eurostat construction output revision counts differ"
            )
        dataset_counts = _current_dataset_comparison_counts(
            releases, self.current_dataset
        )
        if dataset_counts != (
            self.current_dataset_comparison_count,
            self.changed_current_dataset_count,
        ):
            raise ValueError(
                "Eurostat construction output dataset comparison differs"
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
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_ARTIFACTS,
            ),
            _bounded_int(
                self.unique_content_sha256_count,
                "unique_content_sha256_count",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_ARTIFACTS,
            ),
            _bounded_int(
                self.total_content_bytes,
                "total_content_bytes",
                MAX_EUROSTAT_CONSTRUCTION_OUTPUT_TOTAL_BYTES,
            ),
        )
        if counts != (
            expected_artifact_count,
            expected_unique_count,
            expected_bytes,
        ):
            raise ValueError(
                "Eurostat construction output artifact totals differ"
            )
        object.__setattr__(self, "as_of_date", as_of)
        object.__setattr__(self, "registry_id", registry_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "releases", releases)
        object.__setattr__(
            self, "searchable_release_gap_reference_periods", gaps
        )
        expected = _stable_id(
            "eurostat-construction-output-archive-manifest",
            self.identity_payload(),
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError(
                "Eurostat construction output manifest identity differs"
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
    ) -> EurostatConstructionOutputArchiveManifestV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            inventory=EurostatConstructionOutputInventoryV1.from_dict(
                _mapping(data.get("inventory"), "inventory")
            ),
            current_dataset=EurostatConstructionOutputDatasetV1.from_dict(
                _mapping(data.get("current_dataset"), "current_dataset")
            ),
            releases=tuple(
                EurostatConstructionOutputReleaseV1.from_dict(
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
    ) -> EurostatConstructionOutputArchiveManifestV1:
        try:
            data = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "Eurostat construction output manifest is invalid JSON"
            ) from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_eurostat_construction_output_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    dataset_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> EurostatConstructionOutputArchiveManifestV1:
    """Build the construction output receipt from exact retained Eurostat bytes."""
    source = registry.source(EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_KEY)
    if (
        source.parser_id != EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_ID
        or source.parser_version != EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_VERSION
    ):
        raise ValueError(
            "Eurostat construction output registry parser identity differs"
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
            "Eurostat construction output snapshot source identity differs"
        )
    inventory = parse_eurostat_construction_output_inventory(
        search_values, as_of_date=as_of_date
    )
    dataset = parse_eurostat_construction_output_dataset(dataset_snapshot)
    snapshots = release_values
    if len(snapshots) != EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_COUNT:
        raise ValueError(
            "Eurostat construction output release snapshot count differs"
        )
    by_code: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in snapshots:
        code = _product_code(snapshot.request.uri)
        if code in by_code:
            raise ValueError(
                "Eurostat construction output release corpus repeats a product code"
            )
        by_code[code] = snapshot
    if set(by_code) != {item.product_code for item in inventory.entries}:
        raise ValueError(
            "Eurostat construction output release corpus is incomplete"
        )
    if len(document_values) != EUROSTAT_CONSTRUCTION_OUTPUT_DOCUMENT_COUNT:
        raise ValueError(
            "Eurostat construction output document snapshot count differs"
        )
    documents_by_uri: dict[str, OfficialRawSnapshotV1] = {}
    for snapshot in document_values:
        if snapshot.request.uri in documents_by_uri:
            raise ValueError(
                "Eurostat construction output document corpus repeats a URI"
            )
        documents_by_uri[snapshot.request.uri] = snapshot
    used_document_uris: set[str] = set()
    parsed: list[
        tuple[
            EurostatConstructionOutputInventoryEntryV1,
            EurostatConstructionOutputArtifactV1,
            EurostatConstructionOutputArtifactV1 | None,
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
                "Eurostat construction output release request identity differs"
            )
        artifact = _artifact_from_snapshot(
            snapshot,
            EurostatConstructionOutputArtifactRole.RELEASE_HTML,
            product_code=entry.product_code,
        )
        _, page_date, published_at, document_uris = _parse_release_landing(
            snapshot, entry
        )
        document_artifact: EurostatConstructionOutputArtifactV1 | None = None
        document_snapshot: OfficialRawSnapshotV1 | None = None
        if document_uris:
            document_uri = document_uris[0]
            try:
                document_snapshot = documents_by_uri[document_uri]
            except KeyError as exc:
                raise ValueError(
                    "Eurostat construction output document corpus is incomplete"
                ) from exc
            used_document_uris.add(document_uri)
            document_role = {
                OfficialSourceFormat.HTML: (
                    EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_HTML
                ),
                OfficialSourceFormat.PDF: (
                    EurostatConstructionOutputArtifactRole.RELEASE_DOCUMENT_PDF
                ),
            }.get(document_snapshot.request.source_format)
            if document_role is None:
                raise ValueError(
                    "Eurostat construction output document format differs"
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
            "Eurostat construction output document corpus has unexpected artifacts"
        )
    releases: list[EurostatConstructionOutputReleaseV1] = []
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
                "Eurostat construction output release lag changed during parsing"
            )
        headline = _headline_value(entry, reference_period)
        document_snapshot = (
            documents_by_uri[document_uris[0]] if document_uris else None
        )
        releases.append(
            EurostatConstructionOutputReleaseV1(
                inventory_entry=entry,
                reference_period=reference_period,
                reference_period_source_stated=(
                    entry.product_code
                    not in EUROSTAT_CONSTRUCTION_OUTPUT_REFERENCE_PERIOD_INFERENCES
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
    return EurostatConstructionOutputArchiveManifestV1(
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
                    EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_MONTHLY_REFERENCE_PERIOD,
                    -1,
                ),
            ),
            *EUROSTAT_CONSTRUCTION_OUTPUT_INTERNAL_GAP_REFERENCE_PERIODS,
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


def replay_eurostat_construction_output_archive(
    registry: OfficialSourceRegistryV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    dataset_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Sequence[OfficialRawSnapshotV1],
    document_snapshots: Sequence[OfficialRawSnapshotV1],
    expected_manifest: EurostatConstructionOutputArchiveManifestV1,
) -> EurostatConstructionOutputArchiveManifestV1:
    """Rebuild the construction output receipt and require byte-for-byte identity."""
    rebuilt = build_eurostat_construction_output_archive_manifest(
        registry,
        search_snapshots,
        dataset_snapshot,
        release_snapshots,
        document_snapshots,
        as_of_date=expected_manifest.as_of_date,
    )
    if rebuilt.to_json() != expected_manifest.to_json():
        raise ValueError(
            "Eurostat construction output replay differs from packaged manifest"
        )
    return rebuilt


def packaged_eurostat_construction_output_manifest_path() -> Path:
    """Return the installed construction output archive-manifest path."""
    return (
        Path(__file__).resolve().parent
        / "assets"
        / "eurostat_construction_output_archive_v1.json"
    )


def load_packaged_eurostat_construction_output_archive_manifest() -> (
    EurostatConstructionOutputArchiveManifestV1
):
    """Load and validate the installed construction output archive manifest."""
    path = packaged_eurostat_construction_output_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError(
            "packaged Eurostat construction output manifest exceeds size bound"
        )
    manifest = EurostatConstructionOutputArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )
    registry = load_packaged_official_source_registry()
    source = registry.source(EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_KEY)
    if (
        manifest.registry_id != registry.registry_id
        or manifest.source_id != source.source_id
    ):
        raise ValueError(
            "packaged Eurostat construction output registry binding differs"
        )
    return manifest


__all__ = [
    "EUROSTAT_CONSTRUCTION_OUTPUT_ARTIFACT_SCHEMA_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_AS_OF_DATE",
    "EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_SCHEMA_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_DATASET_URI",
    "EUROSTAT_CONSTRUCTION_OUTPUT_DOCUMENT_COUNT",
    "EUROSTAT_CONSTRUCTION_OUTPUT_EXCLUSION_SCHEMA_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_MONTHLY_REFERENCE_PERIOD",
    "EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_MONTHLY_RELEASE_DATE",
    "EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_REFERENCE_PERIOD",
    "EUROSTAT_CONSTRUCTION_OUTPUT_FIRST_SEARCH_RELEASE_DATE",
    "EUROSTAT_CONSTRUCTION_OUTPUT_INTERNAL_GAP_REFERENCE_PERIODS",
    "EUROSTAT_CONSTRUCTION_OUTPUT_INVENTORY_ENTRY_SCHEMA_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_INVENTORY_SCHEMA_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_LAST_QUARTERLY_REFERENCE_PERIOD",
    "EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_PACKAGED_RELEASE_DATE",
    "EUROSTAT_CONSTRUCTION_OUTPUT_LATEST_REFERENCE_PERIOD",
    "EUROSTAT_CONSTRUCTION_OUTPUT_MANIFEST_SCHEMA_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_MEASURE_KEY",
    "EUROSTAT_CONSTRUCTION_OUTPUT_MONTHLY_RELEASE_COUNT",
    "EUROSTAT_CONSTRUCTION_OUTPUT_OBSERVATION_SCHEMA_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_PAGE_DATE_OFFSET_EXCEPTIONS",
    "EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_ID",
    "EUROSTAT_CONSTRUCTION_OUTPUT_PARSER_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_PERIOD_HEADER_CORRECTIONS",
    "EUROSTAT_CONSTRUCTION_OUTPUT_PROGRAM_KEY",
    "EUROSTAT_CONSTRUCTION_OUTPUT_PUBLISHED_VALUE_SCHEMA_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_QUARTERLY_RELEASE_COUNT",
    "EUROSTAT_CONSTRUCTION_OUTPUT_REFERENCE_PERIOD_INFERENCES",
    "EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_COUNT",
    "EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_SCHEMA_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_RELEASE_VALUE_SCHEMA_VERSION",
    "EUROSTAT_CONSTRUCTION_OUTPUT_REPEATED_REFERENCE_PERIODS",
    "EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_PAGE_COUNT",
    "EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_RESULT_COUNT",
    "EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_UNIQUE_URI_COUNT",
    "EUROSTAT_CONSTRUCTION_OUTPUT_SEARCH_URI",
    "EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_KEY",
    "EUROSTAT_CONSTRUCTION_OUTPUT_SOURCE_TIMEZONE",
    "EurostatConstructionOutputArchiveManifestV1",
    "EurostatConstructionOutputArtifactRole",
    "EurostatConstructionOutputArtifactV1",
    "EurostatConstructionOutputDatasetV1",
    "EurostatConstructionOutputIndexExclusionV1",
    "EurostatConstructionOutputInventoryEntryV1",
    "EurostatConstructionOutputInventoryV1",
    "EurostatConstructionOutputMeasure",
    "EurostatConstructionOutputMovement",
    "EurostatConstructionOutputObservationV1",
    "EurostatConstructionOutputPublishedValueV1",
    "EurostatConstructionOutputReleaseV1",
    "EurostatConstructionOutputReleaseValueV1",
    "build_eurostat_construction_output_archive_manifest",
    "build_eurostat_construction_output_dataset_request",
    "build_eurostat_construction_output_document_requests",
    "build_eurostat_construction_output_release_requests",
    "build_eurostat_construction_output_search_requests",
    "load_packaged_eurostat_construction_output_archive_manifest",
    "packaged_eurostat_construction_output_manifest_path",
    "parse_eurostat_construction_output_dataset",
    "parse_eurostat_construction_output_inventory",
    "replay_eurostat_construction_output_archive",
]
