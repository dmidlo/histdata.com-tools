"""Deterministic qualification of INSEE monthly business-climate evidence.

The archive retains two exact bounded English Solr inventories, every selected
monthly release page, one exact BDM series-catalog result, and the current
revised BDM series.  Release values remain source-dated occurrences; the BDM
series is a separately labelled latest-state cross-check.  Raw bytes remain in
an operator-owned corpus and the packaged manifest binds their request and
content identities for offline replay.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from histdatacom.market_context.contracts import canonical_contract_json
from histdatacom.market_context.official_sources import (
    OfficialRawSnapshotV1,
    OfficialRequestMethod,
    OfficialSourceEntryV1,
    OfficialSourceFormat,
    OfficialSourceRegistryV1,
    OfficialSourceRequestV1,
)
from histdatacom.runtime_contracts import JSONValue

INSEE_BUSINESS_CLIMATE_SOURCE_KEY: Final = "fr.insee.business-climate"
INSEE_BUSINESS_CLIMATE_PROGRAM_KEY: Final = "fr.insee.business-climate"
INSEE_BUSINESS_CLIMATE_PARSER_ID: Final = "official.insee-business-climate.v1"
INSEE_BUSINESS_CLIMATE_PARSER_VERSION: Final = "1"
INSEE_BUSINESS_CLIMATE_SOURCE_TIMEZONE: Final = "Europe/Paris"
INSEE_BUSINESS_CLIMATE_SEARCH_URI: Final = (
    "https://www.insee.fr/en/solr/consultation"
)
INSEE_BUSINESS_CLIMATE_CATALOG_URI: Final = (
    "https://www.insee.fr/en/statistiques/series/ajax/consultation"
)
INSEE_BUSINESS_CLIMATE_RELEASE_URI_PREFIX: Final = (
    "https://www.insee.fr/en/statistiques/"
)
INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_ID: Final = "001565530"
INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_URI: Final = (
    "https://bdm.insee.fr/series/sdmx/data/SERIES_BDM/001565530"
    "?startPeriod=2000-01&endPeriod=2026-08"
)
INSEE_BUSINESS_CLIMATE_RELEASE_START_PERIOD: Final = "2009-07"
INSEE_BUSINESS_CLIMATE_CURRENT_START_PERIOD: Final = "2000-01"
INSEE_BUSINESS_CLIMATE_LATEST_PACKAGED_PERIOD: Final = "2026-08"
INSEE_BUSINESS_CLIMATE_RELEASE_GAPS: Final = (
    "2009-08",
    "2010-08",
    "2011-08",
)

INSEE_BUSINESS_CLIMATE_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.insee-business-climate-artifact.v1"
)
INSEE_BUSINESS_CLIMATE_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.insee-business-climate-exclusion.v1"
)
INSEE_BUSINESS_CLIMATE_SEARCH_SCHEMA_VERSION: Final = (
    "histdatacom.insee-business-climate-search.v1"
)
INSEE_BUSINESS_CLIMATE_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.insee-business-climate-value.v1"
)
INSEE_BUSINESS_CLIMATE_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.insee-business-climate-release.v1"
)
INSEE_BUSINESS_CLIMATE_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.insee-business-climate-current-observation.v1"
)
INSEE_BUSINESS_CLIMATE_SERIES_SCHEMA_VERSION: Final = (
    "histdatacom.insee-business-climate-current-series.v1"
)
INSEE_BUSINESS_CLIMATE_COMPARISON_SCHEMA_VERSION: Final = (
    "histdatacom.insee-business-climate-comparison.v1"
)
INSEE_BUSINESS_CLIMATE_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.insee-business-climate-archive-manifest.v1"
)

MAX_INSEE_BUSINESS_CLIMATE_RESULTS: Final = 1_000
MAX_INSEE_BUSINESS_CLIMATE_ARTIFACT_BYTES: Final = 64 * 1024 * 1024
MAX_INSEE_BUSINESS_CLIMATE_TOTAL_BYTES: Final = 512 * 1024 * 1024
_SPACE_RE = re.compile(r"\s+")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMBER_RE = re.compile(r"^[+\-]?\d+(?:[.,]\d+)?$")
_MONTH_RE = re.compile(r"^(?P<year>\d{4})-(?P<month>0[1-9]|1[0-2])$")
_SUBTITLE_RE = re.compile(
    r"^(?:Business climate indicator and turning point indicator|"
    r"Business indicator) - "
    r"(?P<month>[A-Za-z]+) (?P<year>\d{4})\s*$",
    re.IGNORECASE,
)
_MONTH_NUMBER: Final = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "septembre": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_SEARCH_QUERIES: Final = (
    '"Business climate indicator and turning point indicator"',
    '"Business indicator"',
)
_SEARCH_BODIES: Final[tuple[Mapping[str, JSONValue], ...]] = tuple(
    {
        "facetsQuery": [],
        "filters": [
            {"field": "diffusion", "values": [True]},
            {"field": "rubrique", "values": ["statistiques"]},
        ],
        "q": query,
        "rows": 1000,
        "sortFields": [{"field": "dateDiffusion", "order": "desc"}],
        "start": 0,
    }
    for query in _SEARCH_QUERIES
)
_CATALOG_BODY: Final[Mapping[str, JSONValue]] = {
    "facetsField": [],
    "filters": [{"field": "bdm_idFamille", "values": ["103047029"]}],
    "q": "French business climate composite indicator",
    "rows": 1000,
    "sortFields": [],
    "start": 0,
}
_METHODOLOGY: Final = "source-normalized-composite"


def _text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    result = _SPACE_RE.sub(" ", value.replace("\xa0", " ")).strip()
    if not result or len(result) > 16_384:
        raise ValueError(f"{name} is invalid")
    return result


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return cast(Mapping[str, Any], value)


def _sequence(value: object, name: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError(f"{name} must be an array")
    return value


def _integer(value: object, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 0 <= value <= maximum:
        raise ValueError(f"{name} is outside its bound")
    return value


def _month(value: object, name: str) -> str:
    result = _text(value, name)
    if _MONTH_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-MM")
    return result


def _month_ordinal(value: str) -> int:
    match = cast(re.Match[str], _MONTH_RE.fullmatch(_month(value, "month")))
    return int(match.group("year")) * 12 + int(match.group("month")) - 1


def _month_range(start: str, end: str) -> tuple[str, ...]:
    first = _month_ordinal(start)
    last = _month_ordinal(end)
    if first > last:
        raise ValueError("month range is reversed")
    return tuple(
        f"{ordinal // 12:04d}-{ordinal % 12 + 1:02d}"
        for ordinal in range(first, last + 1)
    )


def _release_periods() -> tuple[str, ...]:
    gaps = set(INSEE_BUSINESS_CLIMATE_RELEASE_GAPS)
    return tuple(
        period
        for period in _month_range(
            INSEE_BUSINESS_CLIMATE_RELEASE_START_PERIOD,
            INSEE_BUSINESS_CLIMATE_LATEST_PACKAGED_PERIOD,
        )
        if period not in gaps
    )


def _iso_date(value: object, name: str) -> str:
    result = _text(value, name)
    if date.fromisoformat(result).isoformat() != result:
        raise ValueError(f"{name} must be a canonical ISO date")
    return result


def _timestamp(value: object, name: str) -> str:
    result = _text(value, name)
    try:
        parsed = datetime.fromisoformat(result.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(
        None
    ):
        raise ValueError(f"{name} must be UTC")
    return result


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(dict(payload)).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _official_uri(value: object, name: str = "source_uri") -> str:
    result = _text(value, name)
    parsed = urlsplit(result)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in {"www.insee.fr", "bdm.insee.fr"}
        or parsed.username is not None
        or parsed.fragment
    ):
        raise ValueError(f"{name} must use an official INSEE HTTPS host")
    return result


def _numeric(value: object, name: str) -> tuple[str, float]:
    lexical = _text(value, name)
    normalized = (
        lexical.replace("−", "-")
        .replace("–", "-")
        .replace("—", "-")
        .replace("‑", "-")
        .replace(",", ".")
        .replace("%", "")
        .strip()
    )
    normalized = re.sub(r"^([+-])\s+", r"\1", normalized)
    if _NUMBER_RE.fullmatch(normalized) is None:
        raise ValueError(f"{name} is not numeric")
    numeric = float(normalized)
    if not math.isfinite(numeric) or not -200.0 <= numeric <= 200.0:
        raise ValueError(f"{name} is outside its bound")
    return lexical, numeric


def _body(payload: Mapping[str, JSONValue]) -> str:
    return str(canonical_contract_json(dict(payload)))


def _source(registry: OfficialSourceRegistryV1) -> OfficialSourceEntryV1:
    source = registry.source(INSEE_BUSINESS_CLIMATE_SOURCE_KEY)
    if (
        source.parser_id != INSEE_BUSINESS_CLIMATE_PARSER_ID
        or source.parser_version != INSEE_BUSINESS_CLIMATE_PARSER_VERSION
    ):
        raise ValueError(
            "INSEE business climate registry parser binding differs"
        )
    return source


def _request(
    registry: OfficialSourceRegistryV1,
    uri: str,
    source_format: OfficialSourceFormat,
    *,
    method: OfficialRequestMethod = OfficialRequestMethod.GET,
    body: Mapping[str, JSONValue] | None = None,
) -> OfficialSourceRequestV1:
    source = _source(registry)
    if urlsplit(_official_uri(uri)).hostname not in source.allowed_hosts:
        raise ValueError(
            "INSEE business climate request host differs from registry"
        )
    if source_format not in source.formats:
        raise ValueError(
            "INSEE business climate request format differs from registry"
        )
    return OfficialSourceRequestV1(
        source_key=source.source_key,
        source_id=source.source_id,
        method=method,
        uri=uri,
        source_format=source_format,
        body_text=_body(body) if body is not None else None,
        body_content_type="application/json" if body is not None else None,
        parser_id=source.parser_id,
        parser_version=source.parser_version,
    )


def build_insee_business_climate_search_requests(
    registry: OfficialSourceRegistryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the two exact bounded Solr requests used for discovery."""
    return tuple(
        _request(
            registry,
            INSEE_BUSINESS_CLIMATE_SEARCH_URI,
            OfficialSourceFormat.JSON,
            method=OfficialRequestMethod.POST,
            body=body,
        )
        for body in _SEARCH_BODIES
    )


def build_insee_business_climate_release_requests(
    registry: OfficialSourceRegistryV1,
    documents: Sequence[Mapping[str, Any]],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact HTML request per selected release document."""
    return tuple(
        _request(
            registry,
            f"{INSEE_BUSINESS_CLIMATE_RELEASE_URI_PREFIX}"
            f"{_integer(item.get('id'), 'document id', 1_000_000_000)}",
            OfficialSourceFormat.HTML,
        )
        for item in documents
    )


def build_insee_business_climate_catalog_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact series-catalog request proving the selected BDM ID."""
    return _request(
        registry,
        INSEE_BUSINESS_CLIMATE_CATALOG_URI,
        OfficialSourceFormat.JSON,
        method=OfficialRequestMethod.POST,
        body=_CATALOG_BODY,
    )


def build_insee_business_climate_current_series_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the bounded latest-state BDM business-climate request."""
    return _request(
        registry,
        INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_URI,
        OfficialSourceFormat.SDMX_21,
    )


class InseeBusinessClimateArtifactRole(str, Enum):
    """Semantic role of one retained official response."""

    RELEASE_SEARCH = "release-search"
    RELEASE_HTML = "release-html"
    SERIES_CATALOG = "series-catalog"
    CURRENT_SDMX = "current-sdmx"


@dataclass(frozen=True, slots=True)
class InseeBusinessClimateArtifactV1:
    """Hash-bound receipt for one exact official response."""

    role: InseeBusinessClimateArtifactRole
    source_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    request_id: str
    schema_version: str = INSEE_BUSINESS_CLIMATE_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_BUSINESS_CLIMATE_ARTIFACT_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE business climate artifact schema"
            )
        if not isinstance(self.role, InseeBusinessClimateArtifactRole):
            raise TypeError("INSEE business climate artifact role is invalid")
        if not isinstance(self.source_format, OfficialSourceFormat):
            raise TypeError("INSEE business climate artifact format is invalid")
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        if _DIGEST_RE.fullmatch(self.content_sha256) is None:
            raise ValueError(
                "INSEE business climate artifact digest is invalid"
            )
        if (
            not 0
            < self.content_length
            <= MAX_INSEE_BUSINESS_CLIMATE_ARTIFACT_BYTES
        ):
            raise ValueError(
                "INSEE business climate artifact length is invalid"
            )
        if (
            re.fullmatch(
                r"official-request:sha256:[0-9a-f]{64}", self.request_id
            )
            is None
        ):
            raise ValueError(
                "INSEE business climate request identity is invalid"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role.value,
            "source_uri": self.source_uri,
            "source_format": self.source_format.value,
            "content_sha256": self.content_sha256,
            "content_length": self.content_length,
            "request_id": self.request_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeBusinessClimateArtifactV1:
        return cls(
            role=InseeBusinessClimateArtifactRole(str(data.get("role", ""))),
            source_uri=str(data.get("source_uri", "")),
            source_format=OfficialSourceFormat(
                str(data.get("source_format", ""))
            ),
            content_sha256=str(data.get("content_sha256", "")),
            content_length=cast(int, data.get("content_length")),
            request_id=str(data.get("request_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _artifact(
    snapshot: OfficialRawSnapshotV1, role: InseeBusinessClimateArtifactRole
) -> InseeBusinessClimateArtifactV1:
    if (
        snapshot.request.source_key != INSEE_BUSINESS_CLIMATE_SOURCE_KEY
        or snapshot.request.parser_id != INSEE_BUSINESS_CLIMATE_PARSER_ID
        or snapshot.request.parser_version
        != INSEE_BUSINESS_CLIMATE_PARSER_VERSION
    ):
        raise ValueError("INSEE business climate snapshot names another source")
    return InseeBusinessClimateArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        request_id=snapshot.request.request_id,
    )


@dataclass(frozen=True, slots=True)
class InseeBusinessClimateExcludedSearchResultV1:
    """One explicitly classified but unselected Solr result."""

    source_document_id: int
    title: str
    subtitle: str | None
    reason: str
    schema_version: str = INSEE_BUSINESS_CLIMATE_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_BUSINESS_CLIMATE_EXCLUSION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE business climate exclusion schema"
            )
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError(
                "INSEE business climate excluded document ID is invalid"
            )
        object.__setattr__(self, "title", _text(self.title, "title"))
        if self.subtitle is not None:
            object.__setattr__(
                self, "subtitle", _text(self.subtitle, "subtitle")
            )
        if self.reason not in {
            "missing-subtitle",
            "outside-packaged-coverage",
            "other-business-climate-product",
            "search-result-outside-release-lineage",
        }:
            raise ValueError(
                "INSEE business climate exclusion reason is invalid"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_document_id": self.source_document_id,
            "title": self.title,
            "subtitle": self.subtitle,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeBusinessClimateExcludedSearchResultV1:
        raw_subtitle = data.get("subtitle")
        return cls(
            source_document_id=cast(int, data.get("source_document_id")),
            title=str(data.get("title", "")),
            subtitle=None if raw_subtitle is None else str(raw_subtitle),
            reason=str(data.get("reason", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeBusinessClimateSearchV1:
    """Complete accounting of both bounded release-search responses."""

    as_of_date: str
    artifacts: tuple[InseeBusinessClimateArtifactV1, ...]
    total_result_count: int
    selected_document_ids: tuple[int, ...]
    exclusions: tuple[InseeBusinessClimateExcludedSearchResultV1, ...]
    schema_version: str = INSEE_BUSINESS_CLIMATE_SEARCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_BUSINESS_CLIMATE_SEARCH_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE business climate search schema")
        object.__setattr__(
            self, "as_of_date", _iso_date(self.as_of_date, "as_of_date")
        )
        artifacts = tuple(self.artifacts)
        if len(artifacts) != len(_SEARCH_BODIES) or any(
            item.role is not InseeBusinessClimateArtifactRole.RELEASE_SEARCH
            for item in artifacts
        ):
            raise ValueError(
                "INSEE business climate search artifact inventory differs"
            )
        if (
            not 0
            < self.total_result_count
            <= MAX_INSEE_BUSINESS_CLIMATE_RESULTS
        ):
            raise ValueError(
                "INSEE business climate search result count is invalid"
            )
        ids = tuple(self.selected_document_ids)
        if len(ids) != len(set(ids)):
            raise ValueError(
                "INSEE business climate selected IDs are not unique"
            )
        excluded_ids = {item.source_document_id for item in self.exclusions}
        if len(ids) + len(excluded_ids) != self.total_result_count:
            raise ValueError(
                "INSEE business climate selected/excluded accounting differs"
            )
        if set(ids) & excluded_ids:
            raise ValueError(
                "INSEE business climate selected and excluded IDs overlap"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "artifacts": [item.to_dict() for item in self.artifacts],
            "total_result_count": self.total_result_count,
            "selected_document_ids": list(self.selected_document_ids),
            "exclusions": [item.to_dict() for item in self.exclusions],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> InseeBusinessClimateSearchV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            artifacts=tuple(
                InseeBusinessClimateArtifactV1.from_dict(
                    _mapping(item, "artifact")
                )
                for item in _sequence(data.get("artifacts"), "artifacts")
            ),
            total_result_count=cast(int, data.get("total_result_count")),
            selected_document_ids=tuple(
                cast(int, item)
                for item in _sequence(
                    data.get("selected_document_ids"), "selected_document_ids"
                )
            ),
            exclusions=tuple(
                InseeBusinessClimateExcludedSearchResultV1.from_dict(
                    _mapping(item, "exclusion")
                )
                for item in _sequence(data.get("exclusions"), "exclusions")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeBusinessClimatePublishedValueV1:
    """One contemporaneous all-sector business-climate indicator."""

    lexical_value: str
    value: float
    locator: str
    methodology: str
    metric: str = "business-climate-summary-indicator"
    unit: str = "normalized-index-long-term-average-100"
    adjustment: str = "source-defined-composite-adjustment"
    geography_scope: str = "metropolitan-france"
    schema_version: str = INSEE_BUSINESS_CLIMATE_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_BUSINESS_CLIMATE_VALUE_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE business climate value schema")
        lexical, parsed = _numeric(self.lexical_value, "lexical_value")
        object.__setattr__(self, "lexical_value", lexical)
        if not isinstance(self.value, (float, int)) or isinstance(
            self.value, bool
        ):
            raise TypeError("INSEE business climate value must be numeric")
        if abs(float(self.value) - parsed) > 1e-12:
            raise ValueError(
                "INSEE business climate lexical and numeric values differ"
            )
        object.__setattr__(self, "value", float(self.value))
        object.__setattr__(self, "locator", _text(self.locator, "locator"))
        if self.geography_scope != "metropolitan-france":
            raise ValueError("INSEE business climate geography scope differs")
        if self.methodology != _METHODOLOGY:
            raise ValueError("INSEE business climate methodology differs")
        if (
            self.metric,
            self.unit,
            self.adjustment,
        ) != (
            "business-climate-summary-indicator",
            "normalized-index-long-term-average-100",
            "source-defined-composite-adjustment",
        ):
            raise ValueError("INSEE business climate value semantics differ")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "metric": self.metric,
            "unit": self.unit,
            "adjustment": self.adjustment,
            "geography_scope": self.geography_scope,
            "methodology": self.methodology,
            "lexical_value": self.lexical_value,
            "value": self.value,
            "locator": self.locator,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeBusinessClimatePublishedValueV1:
        return cls(
            lexical_value=str(data.get("lexical_value", "")),
            value=float(cast(float, data.get("value"))),
            locator=str(data.get("locator", "")),
            methodology=str(data.get("methodology", "")),
            geography_scope=str(data.get("geography_scope", "")),
            metric=str(data.get("metric", "")),
            unit=str(data.get("unit", "")),
            adjustment=str(data.get("adjustment", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _subtitle_period(subtitle: str) -> str:
    match = _SUBTITLE_RE.fullmatch(_text(subtitle, "subtitle"))
    if match is None:
        raise ValueError(
            "INSEE business climate subtitle is outside the selected lineage"
        )
    month = _MONTH_NUMBER.get(match.group("month").casefold())
    if month is None:
        raise ValueError("INSEE business climate subtitle month differs")
    return f"{match.group('year')}-{month:02d}"


@dataclass(frozen=True, slots=True)
class InseeBusinessClimateReleaseV1:
    """One source-dated INSEE monthly business-climate publication."""

    source_document_id: int
    reference_period: str
    title: str
    subtitle: str
    published_at: str
    published_at_local: str
    source_uri: str
    artifact: InseeBusinessClimateArtifactV1
    value: InseeBusinessClimatePublishedValueV1
    schema_version: str = INSEE_BUSINESS_CLIMATE_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_BUSINESS_CLIMATE_RELEASE_SCHEMA_VERSION:
            raise ValueError(
                "unsupported INSEE business climate release schema"
            )
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError("INSEE business climate document ID is invalid")
        object.__setattr__(
            self,
            "reference_period",
            _month(self.reference_period, "reference_period"),
        )
        if _subtitle_period(self.subtitle) != self.reference_period:
            raise ValueError("INSEE business climate subtitle period differs")
        object.__setattr__(self, "title", _text(self.title, "title"))
        object.__setattr__(self, "subtitle", _text(self.subtitle, "subtitle"))
        object.__setattr__(
            self, "published_at", _timestamp(self.published_at, "published_at")
        )
        local = _text(self.published_at_local, "published_at_local")
        parsed_local = datetime.fromisoformat(local)
        parsed_utc = datetime.fromisoformat(
            self.published_at.replace("Z", "+00:00")
        )
        if (
            parsed_local.tzinfo is None
            or parsed_local.astimezone(timezone.utc) != parsed_utc
        ):
            raise ValueError(
                "INSEE business climate local publication timestamp differs"
            )
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        expected_uri = f"{INSEE_BUSINESS_CLIMATE_RELEASE_URI_PREFIX}{self.source_document_id}"
        if (
            self.source_uri != expected_uri
            or self.artifact.source_uri != expected_uri
        ):
            raise ValueError("INSEE business climate release URI differs")
        if (
            self.artifact.role
            is not InseeBusinessClimateArtifactRole.RELEASE_HTML
        ):
            raise ValueError(
                "INSEE business climate release artifact role differs"
            )

    @property
    def release_id(self) -> str:
        return _stable_id(
            "insee-business-climate-release", self.identity_payload()
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_document_id": self.source_document_id,
            "reference_period": self.reference_period,
            "title": self.title,
            "subtitle": self.subtitle,
            "published_at": self.published_at,
            "published_at_local": self.published_at_local,
            "source_uri": self.source_uri,
            "artifact": self.artifact.to_dict(),
            "value": self.value.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeBusinessClimateReleaseV1:
        value = cls(
            source_document_id=cast(int, data.get("source_document_id")),
            reference_period=str(data.get("reference_period", "")),
            title=str(data.get("title", "")),
            subtitle=str(data.get("subtitle", "")),
            published_at=str(data.get("published_at", "")),
            published_at_local=str(data.get("published_at_local", "")),
            source_uri=str(data.get("source_uri", "")),
            artifact=InseeBusinessClimateArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            value=InseeBusinessClimatePublishedValueV1.from_dict(
                _mapping(data.get("value"), "value")
            ),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("release_id") != value.release_id:
            raise ValueError("INSEE business climate release identity differs")
        return value


@dataclass(frozen=True, slots=True)
class InseeBusinessClimateCurrentObservationV1:
    """One latest-revised observation from the BDM climate series."""

    reference_period: str
    lexical_value: str
    value: float
    status: str
    schema_version: str = INSEE_BUSINESS_CLIMATE_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_BUSINESS_CLIMATE_OBSERVATION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE business climate observation schema"
            )
        object.__setattr__(
            self,
            "reference_period",
            _month(self.reference_period, "reference_period"),
        )
        lexical, numeric = _numeric(self.lexical_value, "lexical_value")
        object.__setattr__(self, "lexical_value", lexical)
        if abs(float(self.value) - numeric) > 1e-12:
            raise ValueError(
                "INSEE business climate observation lexical differs"
            )
        object.__setattr__(self, "value", float(self.value))
        object.__setattr__(self, "status", _text(self.status, "status"))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "lexical_value": self.lexical_value,
            "value": self.value,
            "status": self.status,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeBusinessClimateCurrentObservationV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            lexical_value=str(data.get("lexical_value", "")),
            value=float(cast(float, data.get("value"))),
            status=str(data.get("status", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeBusinessClimateCurrentSeriesV1:
    """Latest revised BDM series, explicitly separate from release vintages."""

    series_id: str
    title: str
    last_update: str
    observations: tuple[InseeBusinessClimateCurrentObservationV1, ...]
    artifact: InseeBusinessClimateArtifactV1
    metric: str = "business-climate-summary-indicator"
    unit: str = "normalized-index-long-term-average-100"
    adjustment: str = "source-defined-composite-adjustment"
    geography_scope: str = "metropolitan-france"
    methodology: str = _METHODOLOGY
    semantics: str = "latest-revised-cross-check"
    schema_version: str = INSEE_BUSINESS_CLIMATE_SERIES_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_BUSINESS_CLIMATE_SERIES_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE business climate series schema")
        if self.series_id != INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_ID:
            raise ValueError(
                "INSEE business climate current series identity differs"
            )
        object.__setattr__(self, "title", _text(self.title, "title"))
        object.__setattr__(
            self, "last_update", _iso_date(self.last_update, "last_update")
        )
        if (
            self.metric,
            self.unit,
            self.adjustment,
            self.geography_scope,
            self.methodology,
            self.semantics,
        ) != (
            "business-climate-summary-indicator",
            "normalized-index-long-term-average-100",
            "source-defined-composite-adjustment",
            "metropolitan-france",
            _METHODOLOGY,
            "latest-revised-cross-check",
        ):
            raise ValueError(
                "INSEE business climate current series semantics differ"
            )
        values = tuple(self.observations)
        expected = _month_range(
            INSEE_BUSINESS_CLIMATE_CURRENT_START_PERIOD,
            INSEE_BUSINESS_CLIMATE_LATEST_PACKAGED_PERIOD,
        )
        if tuple(item.reference_period for item in values) != expected:
            raise ValueError(
                "INSEE business climate current-series coverage differs"
            )
        if (
            self.artifact.role
            is not InseeBusinessClimateArtifactRole.CURRENT_SDMX
        ):
            raise ValueError(
                "INSEE business climate current-series artifact role differs"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "series_id": self.series_id,
            "title": self.title,
            "last_update": self.last_update,
            "metric": self.metric,
            "unit": self.unit,
            "adjustment": self.adjustment,
            "geography_scope": self.geography_scope,
            "methodology": self.methodology,
            "semantics": self.semantics,
            "observations": [item.to_dict() for item in self.observations],
            "artifact": self.artifact.to_dict(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeBusinessClimateCurrentSeriesV1:
        return cls(
            series_id=str(data.get("series_id", "")),
            title=str(data.get("title", "")),
            last_update=str(data.get("last_update", "")),
            observations=tuple(
                InseeBusinessClimateCurrentObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            artifact=InseeBusinessClimateArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            metric=str(data.get("metric", "")),
            unit=str(data.get("unit", "")),
            adjustment=str(data.get("adjustment", "")),
            geography_scope=str(data.get("geography_scope", "")),
            methodology=str(data.get("methodology", "")),
            semantics=str(data.get("semantics", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeBusinessClimateComparisonV1:
    """Exact-scope release versus latest-revised BDM comparison."""

    reference_period: str
    release_value: float
    current_value: float
    delta: float
    methodology: str = _METHODOLOGY
    schema_version: str = INSEE_BUSINESS_CLIMATE_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_BUSINESS_CLIMATE_COMPARISON_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE business climate comparison schema"
            )
        object.__setattr__(
            self,
            "reference_period",
            _month(self.reference_period, "reference_period"),
        )
        if self.methodology != _METHODOLOGY:
            raise ValueError("INSEE business climate comparison method differs")
        expected = round(
            float(self.current_value) - float(self.release_value), 12
        )
        if abs(float(self.delta) - expected) > 1e-12:
            raise ValueError("INSEE business climate comparison delta differs")
        object.__setattr__(self, "release_value", float(self.release_value))
        object.__setattr__(self, "current_value", float(self.current_value))
        object.__setattr__(self, "delta", float(self.delta))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "methodology": self.methodology,
            "release_value": self.release_value,
            "current_value": self.current_value,
            "delta": self.delta,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeBusinessClimateComparisonV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            release_value=float(cast(float, data.get("release_value"))),
            current_value=float(cast(float, data.get("current_value"))),
            delta=float(cast(float, data.get("delta"))),
            methodology=str(data.get("methodology", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class _HtmlTable:
    table_id: str
    caption: str
    rows: tuple[tuple[str, ...], ...]


class _ReleaseHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.text: list[str] = []
        self.meta_description: str | None = None
        self.tables: list[_HtmlTable] = []
        self._table_id: str | None = None
        self._caption: list[str] | None = None
        self._rows: list[tuple[str, ...]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        values = dict(attrs)
        if (
            tag == "meta"
            and (values.get("name") or "").casefold() == "description"
        ):
            content = values.get("content")
            if content and self.meta_description is None:
                self.meta_description = _SPACE_RE.sub(" ", content).strip()
        elif tag == "table":
            self._table_id = values.get("id") or "<none>"
            self._rows = []
        elif self._table_id is not None and tag == "caption":
            self._caption = []
        elif self._table_id is not None and tag == "tr":
            self._row = []
        elif self._row is not None and tag in {"th", "td"}:
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        if (
            tag in {"th", "td"}
            and self._cell is not None
            and self._row is not None
        ):
            self._row.append(_SPACE_RE.sub(" ", " ".join(self._cell)).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            while self._row and not self._row[-1]:
                self._row.pop()
            if self._row:
                self._rows.append(tuple(self._row))
            self._row = None
        elif tag == "caption" and self._caption is not None:
            self._caption = [
                _SPACE_RE.sub(" ", " ".join(self._caption)).strip()
            ]
        elif tag == "table" and self._table_id is not None:
            self.tables.append(
                _HtmlTable(
                    self._table_id,
                    self._caption[0] if self._caption else "",
                    tuple(self._rows),
                )
            )
            self._table_id = None
            self._caption = None
            self._rows = []

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.text.append(data)
            if self._caption is not None:
                self._caption.append(data)
            if self._cell is not None:
                self._cell.append(data)


def _parse_html(content: bytes) -> _ReleaseHtmlParser:
    try:
        source = content.decode("utf-8")
    except UnicodeError as exc:
        raise ValueError("INSEE business climate release is not UTF-8") from exc
    parser = _ReleaseHtmlParser()
    parser.feed(source)
    parser.close()
    return parser


_LEGACY_MONTH_RE = re.compile(
    r"^(?P<month>[A-Za-zÀ-ÿ]{3,9})\.?\s*(?P<year>\d{2,4})$"
)
_LEGACY_MONTH_NUMBER: Final = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "fev": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "avr": 4,
    "mai": 5,
    "may": 5,
    "jun": 6,
    "june": 6,
    "juin": 6,
    "juil": 7,
    "jul": 7,
    "july": 7,
    "aout": 8,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}


def _legacy_table_period(value: str) -> str | None:
    match = _LEGACY_MONTH_RE.fullmatch(_SPACE_RE.sub(" ", value).strip())
    if match is None:
        return None
    month_name = "".join(
        character
        for character in unicodedata.normalize(
            "NFKD", match.group("month").casefold()
        )
        if not unicodedata.combining(character)
    )
    month = _LEGACY_MONTH_NUMBER.get(month_name)
    if month is None:
        return None
    year = int(match.group("year"))
    if year < 100:
        year += 2000
    return f"{year:04d}-{month:02d}"


def _release_value(
    parser: _ReleaseHtmlParser, period: str
) -> InseeBusinessClimatePublishedValueV1:
    for table_index, table in enumerate(parser.tables):
        if not table.rows:
            continue
        caption = table.caption.casefold()
        if "french business climate composite indicator" in caption:
            for row_index, row in enumerate(table.rows[1:], 1):
                if len(row) >= 2 and row[0] == period:
                    lexical, numeric = _numeric(
                        row[1], "business-climate composite value"
                    )
                    return InseeBusinessClimatePublishedValueV1(
                        lexical_value=lexical,
                        value=numeric,
                        methodology=_METHODOLOGY,
                        locator=(
                            f"table:{table_index}:{table.table_id}:"
                            f"row:{row_index}:column:1"
                        ),
                    )
        header = table.rows[0]
        columns = tuple(
            index
            for index, cell in enumerate(header)
            if _legacy_table_period(cell) == period
        )
        if len(columns) != 1:
            continue
        column = columns[0]
        in_business_climates = False
        for row_index, row in enumerate(table.rows[1:], 1):
            if not row:
                continue
            label = row[0].casefold().strip()
            if label in {"business climates", "composite indicators"}:
                in_business_climates = True
                continue
            if label == "turning point indicators":
                in_business_climates = False
                continue
            if in_business_climates and label == "france" and column < len(row):
                lexical, numeric = _numeric(
                    row[column], "business-climate legacy value"
                )
                return InseeBusinessClimatePublishedValueV1(
                    lexical_value=lexical,
                    value=numeric,
                    methodology=_METHODOLOGY,
                    locator=(
                        f"table:{table_index}:{table.table_id}:"
                        f"row:{row_index}:column:{column}"
                    ),
                )
    page_text = _SPACE_RE.sub(" ", " ".join(parser.text)).strip()
    for source, locator in (
        (parser.meta_description or "", "meta-description"),
        (page_text, "page-introduction"),
    ):
        match = re.search(
            r"\b(?:reaches(?: now)?|stays at|at)\s+"
            r"(?P<value>\d+(?:[.,]\d+)?)\b",
            source,
            re.IGNORECASE,
        )
        if match is not None:
            lexical, numeric = _numeric(
                match.group("value"), "business-climate introductory value"
            )
            return InseeBusinessClimatePublishedValueV1(
                lexical_value=lexical,
                value=numeric,
                methodology=_METHODOLOGY,
                locator=f"paragraph:{locator}:business-climate-summary",
            )
    raise ValueError(
        f"INSEE business climate release omits its summary value for {period}"
    )


def parse_insee_business_climate_search(
    snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> tuple[InseeBusinessClimateSearchV1, tuple[Mapping[str, Any], ...]]:
    """Classify both exact Solr responses and retain their full lineage."""
    boundary = _iso_date(as_of_date, "as_of_date")
    retained = tuple(snapshots)
    if len(retained) != len(_SEARCH_BODIES):
        raise ValueError("INSEE business climate search inventory differs")
    selected: list[Mapping[str, Any]] = []
    exclusions: list[InseeBusinessClimateExcludedSearchResultV1] = []
    seen: set[int] = set()
    total = 0
    artifacts: list[InseeBusinessClimateArtifactV1] = []
    for snapshot, body in zip(retained, _SEARCH_BODIES, strict=True):
        if (
            snapshot.request.method is not OfficialRequestMethod.POST
            or snapshot.request.body_text != _body(body)
        ):
            raise ValueError("INSEE business climate search request differs")
        try:
            payload = json.loads(snapshot.content)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "INSEE business climate search JSON is invalid"
            ) from exc
        root = _mapping(payload, "search")
        documents = tuple(
            _mapping(item, "search document")
            for item in _sequence(root.get("documents"), "documents")
        )
        result_count = _integer(
            root.get("numFounds"),
            "numFounds",
            MAX_INSEE_BUSINESS_CLIMATE_RESULTS,
        )
        if result_count != len(documents):
            raise ValueError(
                "INSEE business climate Solr response is truncated"
            )
        total += result_count
        artifacts.append(
            _artifact(snapshot, InseeBusinessClimateArtifactRole.RELEASE_SEARCH)
        )
        for document in documents:
            document_id = _integer(
                document.get("id"), "document id", 1_000_000_000
            )
            if document_id in seen:
                raise ValueError(
                    "INSEE business climate Solr responses repeat a document"
                )
            seen.add(document_id)
            title = _text(document.get("titre"), "title")
            raw_subtitle = document.get("sousTitre")
            subtitle = (
                None
                if raw_subtitle is None or not str(raw_subtitle).strip()
                else _text(raw_subtitle, "subtitle")
            )
            match = _SUBTITLE_RE.fullmatch(subtitle or "")
            if match is not None:
                period = _subtitle_period(cast(str, subtitle))
                published = _timestamp(
                    document.get("dateDiffusion"), "dateDiffusion"
                )
                published_date = (
                    datetime.fromisoformat(published.replace("Z", "+00:00"))
                    .date()
                    .isoformat()
                )
                if period in _release_periods() and published_date <= boundary:
                    selected.append(document)
                    continue
                reason = "outside-packaged-coverage"
            elif subtitle is None:
                reason = "missing-subtitle"
            elif "business" in f"{title} {subtitle or ''}".casefold():
                reason = "other-business-climate-product"
            else:
                reason = "search-result-outside-release-lineage"
            exclusions.append(
                InseeBusinessClimateExcludedSearchResultV1(
                    document_id, title, subtitle, reason
                )
            )
    selected.sort(key=lambda item: _subtitle_period(str(item["sousTitre"])))
    search = InseeBusinessClimateSearchV1(
        as_of_date=boundary,
        artifacts=tuple(artifacts),
        total_result_count=total,
        selected_document_ids=tuple(cast(int, item["id"]) for item in selected),
        exclusions=tuple(
            sorted(exclusions, key=lambda item: item.source_document_id)
        ),
    )
    return search, tuple(selected)


def _parse_release(
    document: Mapping[str, Any], snapshot: OfficialRawSnapshotV1
) -> InseeBusinessClimateReleaseV1:
    document_id = _integer(document.get("id"), "document id", 1_000_000_000)
    title = _text(document.get("titre"), "title")
    subtitle = _text(document.get("sousTitre"), "subtitle")
    period = _subtitle_period(subtitle)
    published = _timestamp(document.get("dateDiffusion"), "dateDiffusion")
    local = datetime.fromisoformat(published.replace("Z", "+00:00")).astimezone(
        ZoneInfo(INSEE_BUSINESS_CLIMATE_SOURCE_TIMEZONE)
    )
    parser = _parse_html(snapshot.content)
    page_text = _SPACE_RE.sub(" ", " ".join(parser.text)).strip()
    if title not in page_text and subtitle not in page_text:
        raise ValueError(
            f"INSEE business climate page metadata differs: {document_id}"
        )
    return InseeBusinessClimateReleaseV1(
        source_document_id=document_id,
        reference_period=period,
        title=title,
        subtitle=subtitle,
        published_at=published,
        published_at_local=local.isoformat(),
        source_uri=f"{INSEE_BUSINESS_CLIMATE_RELEASE_URI_PREFIX}{document_id}",
        artifact=_artifact(
            snapshot, InseeBusinessClimateArtifactRole.RELEASE_HTML
        ),
        value=_release_value(parser, period),
    )


def _parse_catalog(
    snapshot: OfficialRawSnapshotV1,
) -> InseeBusinessClimateArtifactV1:
    if (
        snapshot.request.method is not OfficialRequestMethod.POST
        or snapshot.request.body_text != _body(_CATALOG_BODY)
    ):
        raise ValueError("INSEE business climate catalog request differs")
    try:
        payload = json.loads(snapshot.content)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "INSEE business climate catalog JSON is invalid"
        ) from exc
    root = _mapping(payload, "catalog")
    documents = tuple(
        _mapping(item, "catalog document")
        for item in _sequence(root.get("documents"), "documents")
    )
    total = _integer(root.get("numFounds"), "numFounds", 1_000)
    if total != len(documents) or total != 3:
        raise ValueError("INSEE business climate catalog result count differs")
    by_id = {str(item.get("idBank")): item for item in documents}
    if set(by_id) != {
        "001565530",
        "001565531",
        "001796629",
    }:
        raise ValueError("INSEE business climate catalog inventory differs")
    document = by_id[INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_ID]
    if (
        document.get("idBank") != INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_ID
        or _text(document.get("titre"), "catalog title")
        != "Business climate summary indicator - All sectors - Metropolitan "
        "France"
    ):
        raise ValueError(
            "INSEE business climate catalog series identity differs"
        )
    return _artifact(snapshot, InseeBusinessClimateArtifactRole.SERIES_CATALOG)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_current_series(
    snapshot: OfficialRawSnapshotV1,
) -> InseeBusinessClimateCurrentSeriesV1:
    try:
        root = ET.fromstring(snapshot.content)
    except ET.ParseError as exc:
        raise ValueError("INSEE business climate SDMX XML is invalid") from exc
    matches = [
        item
        for item in root.iter()
        if _local_name(item.tag) == "Series"
        and item.attrib.get("IDBANK")
        == INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_ID
    ]
    if len(matches) != 1:
        raise ValueError("INSEE business climate SDMX series identity differs")
    series = matches[0]
    if (
        series.attrib.get("FREQ") != "M"
        or series.attrib.get("UNIT_MEASURE") != "SO"
        or series.attrib.get("REF_AREA") != "FM"
        or series.attrib.get("TITLE_EN")
        != "Business climate summary indicator - All sectors - Metropolitan "
        "France"
    ):
        raise ValueError("INSEE business climate SDMX series semantics differ")
    observations = []
    for item in series:
        if _local_name(item.tag) != "Obs":
            continue
        lexical, numeric = _numeric(item.attrib.get("OBS_VALUE"), "OBS_VALUE")
        observations.append(
            InseeBusinessClimateCurrentObservationV1(
                reference_period=_month(
                    item.attrib.get("TIME_PERIOD"), "TIME_PERIOD"
                ),
                lexical_value=lexical,
                value=numeric,
                status=str(item.attrib.get("OBS_STATUS", "<missing>")),
            )
        )
    observations.sort(key=lambda item: _month_ordinal(item.reference_period))
    return InseeBusinessClimateCurrentSeriesV1(
        series_id=INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_ID,
        title=_text(series.attrib.get("TITLE_EN"), "TITLE_EN"),
        last_update=_iso_date(series.attrib.get("LAST_UPDATE"), "LAST_UPDATE"),
        observations=tuple(observations),
        artifact=_artifact(
            snapshot, InseeBusinessClimateArtifactRole.CURRENT_SDMX
        ),
    )


_LIMITATIONS: Final = (
    "The qualified English monthly release-page lineage begins at 2009-07 and has three genuine source-artifact gaps: August 2009, 2010, and 2011.",
    "The source subtitle changes from 'Business climate indicator and turning point indicator' to 'Business indicator' in January 2021; the archive preserves that presentation boundary without changing the indicator identity.",
    "Release tables preserve the source-authored all-sector normalized composite; comparisons require the exact metric, unit, adjustment, geography, methodology, and reference month.",
    "Release-page values are contemporaneous source-dated occurrences; the BDM series is a latest-revised and backcast cross-check and never replaces a historical vintage.",
    "No official historical market-consensus series was identified, so surprise values are not manufactured.",
)


@dataclass(frozen=True, slots=True)
class InseeBusinessClimateArchiveManifestV1:
    """Complete replayable INSEE monthly business-climate manifest."""

    registry_id: str
    source_id: str
    search: InseeBusinessClimateSearchV1
    releases: tuple[InseeBusinessClimateReleaseV1, ...]
    catalog_artifact: InseeBusinessClimateArtifactV1
    current_series: InseeBusinessClimateCurrentSeriesV1
    comparisons: tuple[InseeBusinessClimateComparisonV1, ...]
    coverage_gaps: tuple[str, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    released_value_count: int
    compatible_comparison_count: int
    limitations: tuple[str, ...] = _LIMITATIONS
    source_key: str = INSEE_BUSINESS_CLIMATE_SOURCE_KEY
    program_key: str = INSEE_BUSINESS_CLIMATE_PROGRAM_KEY
    parser_id: str = INSEE_BUSINESS_CLIMATE_PARSER_ID
    parser_version: str = INSEE_BUSINESS_CLIMATE_PARSER_VERSION
    schema_version: str = INSEE_BUSINESS_CLIMATE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_BUSINESS_CLIMATE_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE business climate manifest schema"
            )
        if (
            self.source_key,
            self.program_key,
            self.parser_id,
            self.parser_version,
        ) != (
            INSEE_BUSINESS_CLIMATE_SOURCE_KEY,
            INSEE_BUSINESS_CLIMATE_PROGRAM_KEY,
            INSEE_BUSINESS_CLIMATE_PARSER_ID,
            INSEE_BUSINESS_CLIMATE_PARSER_VERSION,
        ):
            raise ValueError(
                "INSEE business climate manifest program metadata differs"
            )
        for value, prefix, name in (
            (self.registry_id, "official-source-registry", "registry_id"),
            (self.source_id, "official-source", "source_id"),
        ):
            if re.fullmatch(rf"{prefix}:sha256:[0-9a-f]{{64}}", value) is None:
                raise ValueError(
                    f"INSEE business climate manifest {name} is invalid"
                )
        releases = tuple(self.releases)
        if (
            tuple(item.reference_period for item in releases)
            != _release_periods()
        ):
            raise ValueError("INSEE business climate release coverage differs")
        if tuple(item.source_document_id for item in releases) != (
            self.search.selected_document_ids
        ):
            raise ValueError(
                "INSEE business climate release/search order differs"
            )
        if tuple(self.coverage_gaps) != INSEE_BUSINESS_CLIMATE_RELEASE_GAPS:
            raise ValueError(
                "INSEE business climate source-gap accounting differs"
            )
        if (
            self.catalog_artifact.role
            is not InseeBusinessClimateArtifactRole.SERIES_CATALOG
        ):
            raise ValueError(
                "INSEE business climate catalog artifact role differs"
            )
        expected_comparisons = tuple(
            item.reference_period
            for item in releases
            if item.value.methodology == self.current_series.methodology
        )
        if (
            tuple(item.reference_period for item in self.comparisons)
            != expected_comparisons
        ):
            raise ValueError(
                "INSEE business climate exact-scope comparisons differ"
            )
        artifacts = (
            *self.search.artifacts,
            *(item.artifact for item in releases),
            self.catalog_artifact,
            self.current_series.artifact,
        )
        if self.raw_artifact_count != len(artifacts):
            raise ValueError(
                "INSEE business climate raw artifact count differs"
            )
        if self.unique_content_sha256_count != len(
            {item.content_sha256 for item in artifacts}
        ):
            raise ValueError(
                "INSEE business climate unique artifact count differs"
            )
        if self.total_content_bytes != sum(
            item.content_length for item in artifacts
        ):
            raise ValueError(
                "INSEE business climate total content bytes differ"
            )
        if (
            not 0
            < self.total_content_bytes
            <= MAX_INSEE_BUSINESS_CLIMATE_TOTAL_BYTES
        ):
            raise ValueError(
                "INSEE business climate total content bytes are outside bounds"
            )
        if self.released_value_count != len(releases):
            raise ValueError(
                "INSEE business climate released value count differs"
            )
        if self.compatible_comparison_count != len(self.comparisons):
            raise ValueError("INSEE business climate comparison count differs")
        if tuple(self.limitations) != _LIMITATIONS:
            raise ValueError("INSEE business climate limitations differ")

    @property
    def manifest_id(self) -> str:
        return _stable_id(
            "insee-business-climate-archive-manifest",
            self.identity_payload(),
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "registry_id": self.registry_id,
            "source_id": self.source_id,
            "source_key": self.source_key,
            "program_key": self.program_key,
            "parser_id": self.parser_id,
            "parser_version": self.parser_version,
            "search": self.search.to_dict(),
            "releases": [item.to_dict() for item in self.releases],
            "catalog_artifact": self.catalog_artifact.to_dict(),
            "current_series": self.current_series.to_dict(),
            "comparisons": [item.to_dict() for item in self.comparisons],
            "coverage_gaps": list(self.coverage_gaps),
            "raw_artifact_count": self.raw_artifact_count,
            "unique_content_sha256_count": self.unique_content_sha256_count,
            "total_content_bytes": self.total_content_bytes,
            "released_value_count": self.released_value_count,
            "compatible_comparison_count": self.compatible_comparison_count,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeBusinessClimateArchiveManifestV1:
        value = cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            search=InseeBusinessClimateSearchV1.from_dict(
                _mapping(data.get("search"), "search")
            ),
            releases=tuple(
                InseeBusinessClimateReleaseV1.from_dict(
                    _mapping(item, "release")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            catalog_artifact=InseeBusinessClimateArtifactV1.from_dict(
                _mapping(data.get("catalog_artifact"), "catalog_artifact")
            ),
            current_series=InseeBusinessClimateCurrentSeriesV1.from_dict(
                _mapping(data.get("current_series"), "current_series")
            ),
            comparisons=tuple(
                InseeBusinessClimateComparisonV1.from_dict(
                    _mapping(item, "comparison")
                )
                for item in _sequence(data.get("comparisons"), "comparisons")
            ),
            coverage_gaps=tuple(
                str(item)
                for item in _sequence(
                    data.get("coverage_gaps"), "coverage_gaps"
                )
            ),
            raw_artifact_count=cast(int, data.get("raw_artifact_count")),
            unique_content_sha256_count=cast(
                int, data.get("unique_content_sha256_count")
            ),
            total_content_bytes=cast(int, data.get("total_content_bytes")),
            released_value_count=cast(int, data.get("released_value_count")),
            compatible_comparison_count=cast(
                int, data.get("compatible_comparison_count")
            ),
            limitations=tuple(
                str(item)
                for item in _sequence(data.get("limitations"), "limitations")
            ),
            source_key=str(data.get("source_key", "")),
            program_key=str(data.get("program_key", "")),
            parser_id=str(data.get("parser_id", "")),
            parser_version=str(data.get("parser_version", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("manifest_id") != value.manifest_id:
            raise ValueError("INSEE business climate manifest identity differs")
        return value

    @classmethod
    def from_json(cls, payload: str) -> InseeBusinessClimateArchiveManifestV1:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "INSEE business climate manifest JSON is invalid"
            ) from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_insee_business_climate_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search: InseeBusinessClimateSearchV1,
    documents: Sequence[Mapping[str, Any]],
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    catalog_snapshot: OfficialRawSnapshotV1,
    current_series_snapshot: OfficialRawSnapshotV1,
) -> InseeBusinessClimateArchiveManifestV1:
    """Build and fully validate the deterministic archive manifest."""
    source = _source(registry)
    ids = tuple(
        _integer(item.get("id"), "document id", 1_000_000_000)
        for item in documents
    )
    if ids != search.selected_document_ids or set(release_snapshots) != set(
        ids
    ):
        raise ValueError(
            "INSEE business climate retained release inventory differs"
        )
    releases = tuple(
        _parse_release(document, release_snapshots[document_id])
        for document, document_id in zip(documents, ids, strict=True)
    )
    catalog_artifact = _parse_catalog(catalog_snapshot)
    current_series = _parse_current_series(current_series_snapshot)
    current_by_period = {
        item.reference_period: item.value
        for item in current_series.observations
    }
    comparisons = tuple(
        InseeBusinessClimateComparisonV1(
            reference_period=item.reference_period,
            release_value=item.value.value,
            current_value=current_by_period[item.reference_period],
            delta=round(
                current_by_period[item.reference_period] - item.value.value, 12
            ),
        )
        for item in releases
        if item.value.methodology == current_series.methodology
    )
    artifacts = (
        *search.artifacts,
        *(item.artifact for item in releases),
        catalog_artifact,
        current_series.artifact,
    )
    return InseeBusinessClimateArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        search=search,
        releases=releases,
        catalog_artifact=catalog_artifact,
        current_series=current_series,
        comparisons=comparisons,
        coverage_gaps=INSEE_BUSINESS_CLIMATE_RELEASE_GAPS,
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
        released_value_count=len(releases),
        compatible_comparison_count=len(comparisons),
    )


def replay_insee_business_climate_archive(
    registry: OfficialSourceRegistryV1,
    manifest: InseeBusinessClimateArchiveManifestV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    catalog_snapshot: OfficialRawSnapshotV1,
    current_series_snapshot: OfficialRawSnapshotV1,
) -> InseeBusinessClimateArchiveManifestV1:
    """Rebuild retained evidence and require byte-independent equality."""
    if registry.registry_id != manifest.registry_id:
        raise ValueError(
            "INSEE business climate replay registry identity differs"
        )
    search, documents = parse_insee_business_climate_search(
        search_snapshots, as_of_date=manifest.search.as_of_date
    )
    rebuilt = build_insee_business_climate_archive_manifest(
        registry,
        search,
        documents,
        release_snapshots,
        catalog_snapshot,
        current_series_snapshot,
    )
    if rebuilt != manifest:
        raise ValueError(
            "INSEE business climate replay differs from retained manifest"
        )
    return rebuilt


def packaged_insee_business_climate_manifest_path() -> Path:
    """Return the packaged INSEE monthly business-climate manifest path."""
    return (
        Path(__file__).with_name("assets")
        / "insee_business_climate_archive_v1.json"
    )


def load_packaged_insee_business_climate_archive_manifest() -> (
    InseeBusinessClimateArchiveManifestV1
):
    """Load and fully validate the packaged archive."""
    path = packaged_insee_business_climate_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError(
            "packaged INSEE business climate manifest exceeds size bound"
        )
    return InseeBusinessClimateArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )


__all__ = [
    "INSEE_BUSINESS_CLIMATE_CATALOG_URI",
    "INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_ID",
    "INSEE_BUSINESS_CLIMATE_CURRENT_SERIES_URI",
    "INSEE_BUSINESS_CLIMATE_CURRENT_START_PERIOD",
    "INSEE_BUSINESS_CLIMATE_LATEST_PACKAGED_PERIOD",
    "INSEE_BUSINESS_CLIMATE_PARSER_ID",
    "INSEE_BUSINESS_CLIMATE_PARSER_VERSION",
    "INSEE_BUSINESS_CLIMATE_PROGRAM_KEY",
    "INSEE_BUSINESS_CLIMATE_RELEASE_GAPS",
    "INSEE_BUSINESS_CLIMATE_RELEASE_START_PERIOD",
    "INSEE_BUSINESS_CLIMATE_SEARCH_URI",
    "INSEE_BUSINESS_CLIMATE_SOURCE_KEY",
    "InseeBusinessClimateArchiveManifestV1",
    "InseeBusinessClimateArtifactRole",
    "InseeBusinessClimateArtifactV1",
    "InseeBusinessClimateComparisonV1",
    "InseeBusinessClimateCurrentObservationV1",
    "InseeBusinessClimateCurrentSeriesV1",
    "InseeBusinessClimateExcludedSearchResultV1",
    "InseeBusinessClimatePublishedValueV1",
    "InseeBusinessClimateReleaseV1",
    "InseeBusinessClimateSearchV1",
    "build_insee_business_climate_archive_manifest",
    "build_insee_business_climate_catalog_request",
    "build_insee_business_climate_current_series_request",
    "build_insee_business_climate_release_requests",
    "build_insee_business_climate_search_requests",
    "load_packaged_insee_business_climate_archive_manifest",
    "packaged_insee_business_climate_manifest_path",
    "parse_insee_business_climate_search",
    "replay_insee_business_climate_archive",
]
