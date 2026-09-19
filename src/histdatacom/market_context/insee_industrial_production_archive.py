"""Deterministic qualification of INSEE monthly industrial-production evidence.

The archive retains one exact bounded English Solr inventory, every selected
monthly release page, the complete paginated BDM series catalog, and the
current revised manufacturing and whole-industry series. Release changes
remain source-dated occurrences; BDM levels are separately labelled
latest-state cross-checks. Raw bytes remain in an operator-owned corpus and the
packaged manifest binds request and content identities for offline replay.
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

INSEE_INDUSTRIAL_PRODUCTION_SOURCE_KEY: Final = "fr.insee.industrial-production"
INSEE_INDUSTRIAL_PRODUCTION_PROGRAM_KEY: Final = (
    "fr.insee.industrial-production"
)
INSEE_INDUSTRIAL_PRODUCTION_PARSER_ID: Final = (
    "official.insee-industrial-production.v1"
)
INSEE_INDUSTRIAL_PRODUCTION_PARSER_VERSION: Final = "1"
INSEE_INDUSTRIAL_PRODUCTION_SOURCE_TIMEZONE: Final = "Europe/Paris"
INSEE_INDUSTRIAL_PRODUCTION_SEARCH_URI: Final = (
    "https://www.insee.fr/en/solr/consultation"
)
INSEE_INDUSTRIAL_PRODUCTION_CATALOG_URI: Final = (
    "https://www.insee.fr/en/statistiques/series/ajax/consultation"
)
INSEE_INDUSTRIAL_PRODUCTION_RELEASE_URI_PREFIX: Final = (
    "https://www.insee.fr/en/statistiques/"
)
INSEE_INDUSTRIAL_PRODUCTION_INDUSTRY_SERIES_ID: Final = "010768261"
INSEE_INDUSTRIAL_PRODUCTION_MANUFACTURING_SERIES_ID: Final = "010768265"
INSEE_INDUSTRIAL_PRODUCTION_CURRENT_SERIES_IDS: Final = (
    INSEE_INDUSTRIAL_PRODUCTION_INDUSTRY_SERIES_ID,
    INSEE_INDUSTRIAL_PRODUCTION_MANUFACTURING_SERIES_ID,
)
INSEE_INDUSTRIAL_PRODUCTION_RELEASE_START_PERIOD: Final = "2009-04"
INSEE_INDUSTRIAL_PRODUCTION_CURRENT_START_PERIOD: Final = "2000-01"
INSEE_INDUSTRIAL_PRODUCTION_LATEST_PACKAGED_PERIOD: Final = "2026-07"
INSEE_INDUSTRIAL_PRODUCTION_RELEASE_GAPS: Final[tuple[str, ...]] = ()
INSEE_INDUSTRIAL_PRODUCTION_CURRENT_SERIES_URIS: Final = tuple(
    "https://bdm.insee.fr/series/sdmx/data/SERIES_BDM/"
    f"{series_id}?startPeriod=2000-01&endPeriod=2026-07"
    for series_id in INSEE_INDUSTRIAL_PRODUCTION_CURRENT_SERIES_IDS
)

INSEE_INDUSTRIAL_PRODUCTION_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.insee-industrial-production-artifact.v1"
)
INSEE_INDUSTRIAL_PRODUCTION_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.insee-industrial-production-exclusion.v1"
)
INSEE_INDUSTRIAL_PRODUCTION_SEARCH_SCHEMA_VERSION: Final = (
    "histdatacom.insee-industrial-production-search.v1"
)
INSEE_INDUSTRIAL_PRODUCTION_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.insee-industrial-production-value.v1"
)
INSEE_INDUSTRIAL_PRODUCTION_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.insee-industrial-production-release.v1"
)
INSEE_INDUSTRIAL_PRODUCTION_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.insee-industrial-production-current-observation.v1"
)
INSEE_INDUSTRIAL_PRODUCTION_SERIES_SCHEMA_VERSION: Final = (
    "histdatacom.insee-industrial-production-current-series.v1"
)
INSEE_INDUSTRIAL_PRODUCTION_COMPARISON_SCHEMA_VERSION: Final = (
    "histdatacom.insee-industrial-production-comparison.v1"
)
INSEE_INDUSTRIAL_PRODUCTION_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.insee-industrial-production-archive-manifest.v1"
)

MAX_INSEE_INDUSTRIAL_PRODUCTION_RESULTS: Final = 2_000
MAX_INSEE_INDUSTRIAL_PRODUCTION_ARTIFACT_BYTES: Final = 64 * 1024 * 1024
MAX_INSEE_INDUSTRIAL_PRODUCTION_TOTAL_BYTES: Final = 512 * 1024 * 1024
_SPACE_RE = re.compile(r"\s+")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMBER_RE = re.compile(r"^[+\-]?\d+(?:[.,]\d+)?$")
_MONTH_RE = re.compile(r"^(?P<year>\d{4})-(?P<month>0[1-9]|1[0-2])$")
_SUBTITLE_RE = re.compile(
    r"^Industrial production index - "
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
    "novembre": 11,
    "december": 12,
}
_SEARCH_QUERIES: Final = ('"Industrial production index"',)
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
_CATALOG_BODIES: Final[tuple[Mapping[str, JSONValue], ...]] = tuple(
    {
        "facetsField": [],
        "filters": [{"field": "bdm_idFamille", "values": ["117606968"]}],
        "q": "Industrial production index",
        "rows": 1000,
        "sortFields": [],
        "start": start,
    }
    for start in (0, 1000)
)
_RELEASE_METHODOLOGY: Final = "source-published-sa-wda-month-over-month"
_CURRENT_METHODOLOGY: Final = "current-revised-sa-wda-index-level"
_COMPARISON_METHODOLOGY: Final = "derived-current-growth-versus-release-vintage"
_SOURCE_PERIOD_CORRECTIONS: Final = {1_562_488: "2010-12"}
_SOURCE_HEADER_COLUMN_CORRECTIONS: Final = {3_560_105: 1}


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
    gaps = set(INSEE_INDUSTRIAL_PRODUCTION_RELEASE_GAPS)
    return tuple(
        period
        for period in _month_range(
            INSEE_INDUSTRIAL_PRODUCTION_RELEASE_START_PERIOD,
            INSEE_INDUSTRIAL_PRODUCTION_LATEST_PACKAGED_PERIOD,
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
    source = registry.source(INSEE_INDUSTRIAL_PRODUCTION_SOURCE_KEY)
    if (
        source.parser_id != INSEE_INDUSTRIAL_PRODUCTION_PARSER_ID
        or source.parser_version != INSEE_INDUSTRIAL_PRODUCTION_PARSER_VERSION
    ):
        raise ValueError(
            "INSEE industrial production registry parser binding differs"
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
            "INSEE industrial production request host differs from registry"
        )
    if source_format not in source.formats:
        raise ValueError(
            "INSEE industrial production request format differs from registry"
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


def build_insee_industrial_production_search_requests(
    registry: OfficialSourceRegistryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build the exact bounded Solr request used for discovery."""
    return tuple(
        _request(
            registry,
            INSEE_INDUSTRIAL_PRODUCTION_SEARCH_URI,
            OfficialSourceFormat.JSON,
            method=OfficialRequestMethod.POST,
            body=body,
        )
        for body in _SEARCH_BODIES
    )


def build_insee_industrial_production_release_requests(
    registry: OfficialSourceRegistryV1,
    documents: Sequence[Mapping[str, Any]],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact HTML request per selected release document."""
    return tuple(
        _request(
            registry,
            f"{INSEE_INDUSTRIAL_PRODUCTION_RELEASE_URI_PREFIX}"
            f"{_integer(item.get('id'), 'document id', 1_000_000_000)}",
            OfficialSourceFormat.HTML,
        )
        for item in documents
    )


def build_insee_industrial_production_catalog_requests(
    registry: OfficialSourceRegistryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build both exact catalog-page requests proving the BDM inventory."""
    return tuple(
        _request(
            registry,
            INSEE_INDUSTRIAL_PRODUCTION_CATALOG_URI,
            OfficialSourceFormat.JSON,
            method=OfficialRequestMethod.POST,
            body=body,
        )
        for body in _CATALOG_BODIES
    )


def build_insee_industrial_production_current_series_requests(
    registry: OfficialSourceRegistryV1,
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build bounded latest-state BDM requests for both headline series."""
    return tuple(
        _request(registry, uri, OfficialSourceFormat.SDMX_21)
        for uri in INSEE_INDUSTRIAL_PRODUCTION_CURRENT_SERIES_URIS
    )


class InseeIndustrialProductionAggregate(str, Enum):
    """Stable headline aggregate carried across release-base changes."""

    INDUSTRY = "industry-be"
    MANUFACTURING = "manufacturing-cz"


class InseeIndustrialProductionArtifactRole(str, Enum):
    """Semantic role of one retained official response."""

    RELEASE_SEARCH = "release-search"
    RELEASE_HTML = "release-html"
    SERIES_CATALOG = "series-catalog"
    CURRENT_SDMX = "current-sdmx"


@dataclass(frozen=True, slots=True)
class InseeIndustrialProductionArtifactV1:
    """Hash-bound receipt for one exact official response."""

    role: InseeIndustrialProductionArtifactRole
    source_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    request_id: str
    schema_version: str = INSEE_INDUSTRIAL_PRODUCTION_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_INDUSTRIAL_PRODUCTION_ARTIFACT_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE industrial production artifact schema"
            )
        if not isinstance(self.role, InseeIndustrialProductionArtifactRole):
            raise TypeError(
                "INSEE industrial production artifact role is invalid"
            )
        if not isinstance(self.source_format, OfficialSourceFormat):
            raise TypeError(
                "INSEE industrial production artifact format is invalid"
            )
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        if _DIGEST_RE.fullmatch(self.content_sha256) is None:
            raise ValueError(
                "INSEE industrial production artifact digest is invalid"
            )
        if (
            not 0
            < self.content_length
            <= MAX_INSEE_INDUSTRIAL_PRODUCTION_ARTIFACT_BYTES
        ):
            raise ValueError(
                "INSEE industrial production artifact length is invalid"
            )
        if (
            re.fullmatch(
                r"official-request:sha256:[0-9a-f]{64}", self.request_id
            )
            is None
        ):
            raise ValueError(
                "INSEE industrial production request identity is invalid"
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
    ) -> InseeIndustrialProductionArtifactV1:
        return cls(
            role=InseeIndustrialProductionArtifactRole(
                str(data.get("role", ""))
            ),
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
    snapshot: OfficialRawSnapshotV1, role: InseeIndustrialProductionArtifactRole
) -> InseeIndustrialProductionArtifactV1:
    if (
        snapshot.request.source_key != INSEE_INDUSTRIAL_PRODUCTION_SOURCE_KEY
        or snapshot.request.parser_id != INSEE_INDUSTRIAL_PRODUCTION_PARSER_ID
        or snapshot.request.parser_version
        != INSEE_INDUSTRIAL_PRODUCTION_PARSER_VERSION
    ):
        raise ValueError(
            "INSEE industrial production snapshot names another source"
        )
    return InseeIndustrialProductionArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        request_id=snapshot.request.request_id,
    )


@dataclass(frozen=True, slots=True)
class InseeIndustrialProductionExcludedSearchResultV1:
    """One explicitly classified but unselected Solr result."""

    source_document_id: int
    title: str
    subtitle: str | None
    reason: str
    schema_version: str = INSEE_INDUSTRIAL_PRODUCTION_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_INDUSTRIAL_PRODUCTION_EXCLUSION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE industrial production exclusion schema"
            )
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError(
                "INSEE industrial production excluded document ID is invalid"
            )
        object.__setattr__(self, "title", _text(self.title, "title"))
        if self.subtitle is not None:
            object.__setattr__(
                self, "subtitle", _text(self.subtitle, "subtitle")
            )
        if self.reason not in {
            "missing-subtitle",
            "outside-packaged-coverage",
            "other-industrial-production-product",
            "search-result-outside-release-lineage",
        }:
            raise ValueError(
                "INSEE industrial production exclusion reason is invalid"
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
    ) -> InseeIndustrialProductionExcludedSearchResultV1:
        raw_subtitle = data.get("subtitle")
        return cls(
            source_document_id=cast(int, data.get("source_document_id")),
            title=str(data.get("title", "")),
            subtitle=None if raw_subtitle is None else str(raw_subtitle),
            reason=str(data.get("reason", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeIndustrialProductionSearchV1:
    """Complete accounting of the bounded release-search response."""

    as_of_date: str
    artifacts: tuple[InseeIndustrialProductionArtifactV1, ...]
    total_result_count: int
    selected_document_ids: tuple[int, ...]
    exclusions: tuple[InseeIndustrialProductionExcludedSearchResultV1, ...]
    schema_version: str = INSEE_INDUSTRIAL_PRODUCTION_SEARCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_INDUSTRIAL_PRODUCTION_SEARCH_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE industrial production search schema"
            )
        object.__setattr__(
            self, "as_of_date", _iso_date(self.as_of_date, "as_of_date")
        )
        artifacts = tuple(self.artifacts)
        if len(artifacts) != len(_SEARCH_BODIES) or any(
            item.role
            is not InseeIndustrialProductionArtifactRole.RELEASE_SEARCH
            for item in artifacts
        ):
            raise ValueError(
                "INSEE industrial production search artifact inventory differs"
            )
        if (
            not 0
            < self.total_result_count
            <= MAX_INSEE_INDUSTRIAL_PRODUCTION_RESULTS
        ):
            raise ValueError(
                "INSEE industrial production search result count is invalid"
            )
        ids = tuple(self.selected_document_ids)
        if len(ids) != len(set(ids)):
            raise ValueError(
                "INSEE industrial production selected IDs are not unique"
            )
        excluded_ids = {item.source_document_id for item in self.exclusions}
        if len(ids) + len(excluded_ids) != self.total_result_count:
            raise ValueError(
                "INSEE industrial production selected/excluded accounting differs"
            )
        if set(ids) & excluded_ids:
            raise ValueError(
                "INSEE industrial production selected and excluded IDs overlap"
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
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeIndustrialProductionSearchV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            artifacts=tuple(
                InseeIndustrialProductionArtifactV1.from_dict(
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
                InseeIndustrialProductionExcludedSearchResultV1.from_dict(
                    _mapping(item, "exclusion")
                )
                for item in _sequence(data.get("exclusions"), "exclusions")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeIndustrialProductionPublishedValueV1:
    """One contemporaneous headline month-over-month production change."""

    aggregate: InseeIndustrialProductionAggregate
    lexical_value: str
    value: float
    locator: str
    methodology: str = _RELEASE_METHODOLOGY
    metric: str = "industrial-production-month-over-month-change"
    unit: str = "percent"
    adjustment: str = "seasonally-and-working-day-adjusted"
    geography_scope: str = "metropolitan-france"
    schema_version: str = INSEE_INDUSTRIAL_PRODUCTION_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_INDUSTRIAL_PRODUCTION_VALUE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE industrial production value schema"
            )
        lexical, parsed = _numeric(self.lexical_value, "lexical_value")
        object.__setattr__(self, "lexical_value", lexical)
        if not isinstance(self.value, (float, int)) or isinstance(
            self.value, bool
        ):
            raise TypeError("INSEE industrial production value must be numeric")
        if (
            not math.isfinite(self.value)
            or abs(float(self.value) - parsed) > 1e-12
        ):
            raise ValueError(
                "INSEE industrial production lexical and numeric values differ"
            )
        object.__setattr__(self, "value", float(self.value))
        object.__setattr__(self, "locator", _text(self.locator, "locator"))
        if self.geography_scope != "metropolitan-france":
            raise ValueError(
                "INSEE industrial production geography scope differs"
            )
        if not isinstance(self.aggregate, InseeIndustrialProductionAggregate):
            raise TypeError("INSEE industrial production aggregate is invalid")
        if self.methodology != _RELEASE_METHODOLOGY:
            raise ValueError("INSEE industrial production methodology differs")
        if (
            self.metric,
            self.unit,
            self.adjustment,
        ) != (
            "industrial-production-month-over-month-change",
            "percent",
            "seasonally-and-working-day-adjusted",
        ):
            raise ValueError(
                "INSEE industrial production value semantics differ"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "aggregate": self.aggregate.value,
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
    ) -> InseeIndustrialProductionPublishedValueV1:
        return cls(
            aggregate=InseeIndustrialProductionAggregate(
                str(data.get("aggregate", ""))
            ),
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


def _subtitle_period(
    subtitle: str, source_document_id: int | None = None
) -> str:
    if source_document_id in _SOURCE_PERIOD_CORRECTIONS:
        if subtitle != "Industrial production index - December 2011":
            raise ValueError("INSEE corrected source subtitle differs")
        return _SOURCE_PERIOD_CORRECTIONS[source_document_id]
    match = _SUBTITLE_RE.fullmatch(_text(subtitle, "subtitle"))
    if match is None:
        raise ValueError(
            "INSEE industrial production subtitle is outside the selected lineage"
        )
    month = _MONTH_NUMBER.get(match.group("month").casefold())
    if month is None:
        raise ValueError("INSEE industrial production subtitle month differs")
    return f"{match.group('year')}-{month:02d}"


@dataclass(frozen=True, slots=True)
class InseeIndustrialProductionReleaseV1:
    """One source-dated INSEE monthly industrial-production publication."""

    source_document_id: int
    reference_period: str
    title: str
    subtitle: str
    published_at: str
    published_at_local: str
    source_uri: str
    artifact: InseeIndustrialProductionArtifactV1
    values: tuple[InseeIndustrialProductionPublishedValueV1, ...]
    schema_version: str = INSEE_INDUSTRIAL_PRODUCTION_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_INDUSTRIAL_PRODUCTION_RELEASE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE industrial production release schema"
            )
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError(
                "INSEE industrial production document ID is invalid"
            )
        object.__setattr__(
            self,
            "reference_period",
            _month(self.reference_period, "reference_period"),
        )
        if (
            _subtitle_period(self.subtitle, self.source_document_id)
            != self.reference_period
        ):
            raise ValueError(
                "INSEE industrial production subtitle period differs"
            )
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
            or parsed_local.utcoffset()
            != parsed_utc.astimezone(
                ZoneInfo(INSEE_INDUSTRIAL_PRODUCTION_SOURCE_TIMEZONE)
            ).utcoffset()
        ):
            raise ValueError(
                "INSEE industrial production local publication timestamp differs"
            )
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        expected_uri = f"{INSEE_INDUSTRIAL_PRODUCTION_RELEASE_URI_PREFIX}{self.source_document_id}"
        if (
            self.source_uri != expected_uri
            or self.artifact.source_uri != expected_uri
        ):
            raise ValueError("INSEE industrial production release URI differs")
        if (
            self.artifact.role
            is not InseeIndustrialProductionArtifactRole.RELEASE_HTML
        ):
            raise ValueError(
                "INSEE industrial production release artifact role differs"
            )
        values = tuple(self.values)
        if tuple(item.aggregate for item in values) != tuple(
            InseeIndustrialProductionAggregate
        ):
            raise ValueError(
                "INSEE industrial production release aggregate inventory differs"
            )

    @property
    def release_id(self) -> str:
        return _stable_id(
            "insee-industrial-production-release", self.identity_payload()
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
            "values": [item.to_dict() for item in self.values],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeIndustrialProductionReleaseV1:
        value = cls(
            source_document_id=cast(int, data.get("source_document_id")),
            reference_period=str(data.get("reference_period", "")),
            title=str(data.get("title", "")),
            subtitle=str(data.get("subtitle", "")),
            published_at=str(data.get("published_at", "")),
            published_at_local=str(data.get("published_at_local", "")),
            source_uri=str(data.get("source_uri", "")),
            artifact=InseeIndustrialProductionArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            values=tuple(
                InseeIndustrialProductionPublishedValueV1.from_dict(
                    _mapping(item, "value")
                )
                for item in _sequence(data.get("values"), "values")
            ),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("release_id") != value.release_id:
            raise ValueError(
                "INSEE industrial production release identity differs"
            )
        return value


@dataclass(frozen=True, slots=True)
class InseeIndustrialProductionCurrentObservationV1:
    """One latest-revised observation from a BDM production series."""

    reference_period: str
    lexical_value: str
    value: float
    status: str
    schema_version: str = INSEE_INDUSTRIAL_PRODUCTION_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_INDUSTRIAL_PRODUCTION_OBSERVATION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE industrial production observation schema"
            )
        object.__setattr__(
            self,
            "reference_period",
            _month(self.reference_period, "reference_period"),
        )
        lexical, numeric = _numeric(self.lexical_value, "lexical_value")
        object.__setattr__(self, "lexical_value", lexical)
        if (
            not math.isfinite(self.value)
            or abs(float(self.value) - numeric) > 1e-12
        ):
            raise ValueError(
                "INSEE industrial production observation lexical differs"
            )
        if numeric <= 0:
            raise ValueError(
                "INSEE industrial production index must be positive"
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
    ) -> InseeIndustrialProductionCurrentObservationV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            lexical_value=str(data.get("lexical_value", "")),
            value=float(cast(float, data.get("value"))),
            status=str(data.get("status", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeIndustrialProductionCurrentSeriesV1:
    """Latest revised BDM series, explicitly separate from release vintages."""

    aggregate: InseeIndustrialProductionAggregate
    series_id: str
    title: str
    last_update: str
    observations: tuple[InseeIndustrialProductionCurrentObservationV1, ...]
    artifact: InseeIndustrialProductionArtifactV1
    metric: str = "industrial-production-index"
    unit: str = "base-2021-equals-100"
    adjustment: str = "seasonally-and-working-day-adjusted"
    geography_scope: str = "metropolitan-france"
    methodology: str = _CURRENT_METHODOLOGY
    semantics: str = "latest-revised-cross-check"
    schema_version: str = INSEE_INDUSTRIAL_PRODUCTION_SERIES_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_INDUSTRIAL_PRODUCTION_SERIES_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE industrial production series schema"
            )
        expected_id = {
            InseeIndustrialProductionAggregate.INDUSTRY: INSEE_INDUSTRIAL_PRODUCTION_INDUSTRY_SERIES_ID,
            InseeIndustrialProductionAggregate.MANUFACTURING: INSEE_INDUSTRIAL_PRODUCTION_MANUFACTURING_SERIES_ID,
        }.get(self.aggregate)
        if self.series_id != expected_id:
            raise ValueError(
                "INSEE industrial production current series identity differs"
            )
        expected_uri = INSEE_INDUSTRIAL_PRODUCTION_CURRENT_SERIES_URIS[
            INSEE_INDUSTRIAL_PRODUCTION_CURRENT_SERIES_IDS.index(self.series_id)
        ]
        if self.artifact.source_uri != expected_uri:
            raise ValueError(
                "INSEE industrial production current series artifact URI differs"
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
            "industrial-production-index",
            "base-2021-equals-100",
            "seasonally-and-working-day-adjusted",
            "metropolitan-france",
            _CURRENT_METHODOLOGY,
            "latest-revised-cross-check",
        ):
            raise ValueError(
                "INSEE industrial production current series semantics differ"
            )
        values = tuple(self.observations)
        expected = _month_range(
            INSEE_INDUSTRIAL_PRODUCTION_CURRENT_START_PERIOD,
            INSEE_INDUSTRIAL_PRODUCTION_LATEST_PACKAGED_PERIOD,
        )
        if tuple(item.reference_period for item in values) != expected:
            raise ValueError(
                "INSEE industrial production current-series coverage differs"
            )
        if (
            self.artifact.role
            is not InseeIndustrialProductionArtifactRole.CURRENT_SDMX
        ):
            raise ValueError(
                "INSEE industrial production current-series artifact role differs"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "aggregate": self.aggregate.value,
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
    ) -> InseeIndustrialProductionCurrentSeriesV1:
        return cls(
            aggregate=InseeIndustrialProductionAggregate(
                str(data.get("aggregate", ""))
            ),
            series_id=str(data.get("series_id", "")),
            title=str(data.get("title", "")),
            last_update=str(data.get("last_update", "")),
            observations=tuple(
                InseeIndustrialProductionCurrentObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            artifact=InseeIndustrialProductionArtifactV1.from_dict(
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
class InseeIndustrialProductionComparisonV1:
    """Release change versus growth derived from latest-revised BDM levels."""

    aggregate: InseeIndustrialProductionAggregate
    reference_period: str
    release_value: float
    current_value: float
    delta: float
    methodology: str = _COMPARISON_METHODOLOGY
    schema_version: str = INSEE_INDUSTRIAL_PRODUCTION_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_INDUSTRIAL_PRODUCTION_COMPARISON_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE industrial production comparison schema"
            )
        object.__setattr__(
            self,
            "reference_period",
            _month(self.reference_period, "reference_period"),
        )
        if not isinstance(self.aggregate, InseeIndustrialProductionAggregate):
            raise TypeError("INSEE industrial production aggregate is invalid")
        if self.methodology != _COMPARISON_METHODOLOGY:
            raise ValueError(
                "INSEE industrial production comparison method differs"
            )
        if not all(
            math.isfinite(item)
            for item in (self.release_value, self.current_value, self.delta)
        ):
            raise ValueError(
                "INSEE industrial production comparison must be finite"
            )
        expected = round(
            float(self.current_value) - float(self.release_value), 12
        )
        if abs(float(self.delta) - expected) > 1e-12:
            raise ValueError(
                "INSEE industrial production comparison delta differs"
            )
        object.__setattr__(self, "release_value", float(self.release_value))
        object.__setattr__(self, "current_value", float(self.current_value))
        object.__setattr__(self, "delta", float(self.delta))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "aggregate": self.aggregate.value,
            "reference_period": self.reference_period,
            "methodology": self.methodology,
            "release_value": self.release_value,
            "current_value": self.current_value,
            "delta": self.delta,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeIndustrialProductionComparisonV1:
        return cls(
            aggregate=InseeIndustrialProductionAggregate(
                str(data.get("aggregate", ""))
            ),
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
        raise ValueError(
            "INSEE industrial production release is not UTF-8"
        ) from exc
    parser = _ReleaseHtmlParser()
    parser.feed(source)
    parser.close()
    return parser


_TABLE_MONTH_NUMBER: Final = {
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


def _normalized_words(value: str) -> tuple[str, ...]:
    normalized = "".join(
        character
        for character in unicodedata.normalize("NFKD", value.casefold())
        if not unicodedata.combining(character)
    )
    return tuple(re.findall(r"[a-z]+", normalized))


def _table_months(value: str) -> tuple[int, ...]:
    months: list[int] = []
    for word in _normalized_words(value):
        month = _TABLE_MONTH_NUMBER.get(word.rstrip("."))
        if month is None and len(word) >= 3:
            month = _TABLE_MONTH_NUMBER.get(word[:3])
        if month is not None:
            months.append(month)
    return tuple(months)


def _previous_month(period: str) -> int:
    month = int(period[5:])
    return 12 if month == 1 else month - 1


def _is_current_month_change_header(value: str, period: str) -> bool:
    current_month = int(period[5:])
    if _table_months(value)[:2] != (
        current_month,
        _previous_month(period),
    ):
        return False
    years = tuple(int(item) for item in re.findall(r"\b\d{4}\b", value))
    if not years:
        return True
    current_year = int(period[:4])
    previous_year = current_year - 1 if current_month == 1 else current_year
    if len(years) == 1:
        if current_year == previous_year:
            return years[0] == current_year
        parts = value.split("/")
        if len(parts) != 2:
            return False
        return all(
            all(
                int(year) == expected for year in re.findall(r"\b\d{4}\b", part)
            )
            for part, expected in zip(
                parts, (current_year, previous_year), strict=True
            )
        )
    return years == (current_year, previous_year)


def _release_values(
    parser: _ReleaseHtmlParser,
    period: str,
    source_document_id: int,
) -> tuple[InseeIndustrialProductionPublishedValueV1, ...]:
    for table_index, table in enumerate(parser.tables):
        if not table.rows:
            continue
        header = table.rows[0]
        corrected_column = _SOURCE_HEADER_COLUMN_CORRECTIONS.get(
            source_document_id
        )
        if (
            corrected_column is not None
            and table_index == 0
            and (
                period != "2018-04"
                or header[1:3] != ("April / April", "April / Feb.")
            )
        ):
            raise ValueError("INSEE corrected source table header differs")
        columns = (
            (corrected_column,)
            if corrected_column is not None and table_index == 0
            else tuple(
                index
                for index, cell in enumerate(header)
                if _is_current_month_change_header(cell, period)
            )
        )
        if len(columns) != 1:
            continue
        column = columns[0]
        data_column = column
        if len(table.rows) > 1 and len(table.rows[1]) == len(header) + 1:
            data_column += 1
        located: dict[
            InseeIndustrialProductionAggregate,
            InseeIndustrialProductionPublishedValueV1,
        ] = {}
        for row_index, row in enumerate(table.rows[1:], 1):
            if not row or data_column >= len(row):
                continue
            label = _SPACE_RE.sub(" ", row[0]).strip().upper()
            aggregate = None
            if re.match(r"^BE\s*:\s*INDUSTRY\b", label):
                aggregate = InseeIndustrialProductionAggregate.INDUSTRY
            elif re.match(r"^CZ\s*:\s*MANUFACTURING\b", label):
                aggregate = InseeIndustrialProductionAggregate.MANUFACTURING
            if aggregate is not None:
                if aggregate in located:
                    raise ValueError(
                        "INSEE industrial production aggregate row repeats"
                    )
                lexical, numeric = _numeric(
                    row[data_column],
                    f"{aggregate.value} month-over-month change",
                )
                located[aggregate] = InseeIndustrialProductionPublishedValueV1(
                    aggregate=aggregate,
                    lexical_value=lexical,
                    value=numeric,
                    locator=(
                        f"table:{table_index}:{table.table_id}:"
                        f"row:{row_index}:column:{data_column}"
                    ),
                )
        if set(located) == set(InseeIndustrialProductionAggregate):
            return tuple(
                located[item] for item in InseeIndustrialProductionAggregate
            )
    raise ValueError(
        "INSEE industrial production release omits its BE/CZ monthly changes "
        f"for {period}"
    )


def parse_insee_industrial_production_search(
    snapshots: Sequence[OfficialRawSnapshotV1],
    *,
    as_of_date: str,
) -> tuple[InseeIndustrialProductionSearchV1, tuple[Mapping[str, Any], ...]]:
    """Classify the exact Solr response and retain its full lineage."""
    boundary = _iso_date(as_of_date, "as_of_date")
    retained = tuple(snapshots)
    if len(retained) != len(_SEARCH_BODIES):
        raise ValueError("INSEE industrial production search inventory differs")
    selected: list[Mapping[str, Any]] = []
    exclusions: list[InseeIndustrialProductionExcludedSearchResultV1] = []
    seen: set[int] = set()
    total = 0
    artifacts: list[InseeIndustrialProductionArtifactV1] = []
    for snapshot, body in zip(retained, _SEARCH_BODIES, strict=True):
        if (
            snapshot.request.method is not OfficialRequestMethod.POST
            or snapshot.request.body_text != _body(body)
            or snapshot.request.uri != INSEE_INDUSTRIAL_PRODUCTION_SEARCH_URI
            or snapshot.request.source_format is not OfficialSourceFormat.JSON
        ):
            raise ValueError(
                "INSEE industrial production search request differs"
            )
        try:
            payload = json.loads(snapshot.content)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "INSEE industrial production search JSON is invalid"
            ) from exc
        root = _mapping(payload, "search")
        documents = tuple(
            _mapping(item, "search document")
            for item in _sequence(root.get("documents"), "documents")
        )
        result_count = _integer(
            root.get("numFounds"),
            "numFounds",
            MAX_INSEE_INDUSTRIAL_PRODUCTION_RESULTS,
        )
        if result_count != len(documents):
            raise ValueError(
                "INSEE industrial production Solr response is truncated"
            )
        total += result_count
        artifacts.append(
            _artifact(
                snapshot, InseeIndustrialProductionArtifactRole.RELEASE_SEARCH
            )
        )
        for document in documents:
            document_id = _integer(
                document.get("id"), "document id", 1_000_000_000
            )
            if document_id in seen:
                raise ValueError(
                    "INSEE industrial production Solr responses repeat a document"
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
                period = _subtitle_period(cast(str, subtitle), document_id)
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
            elif "industrial production" in (
                f"{title} {subtitle or ''}".casefold()
            ):
                reason = "other-industrial-production-product"
            else:
                reason = "search-result-outside-release-lineage"
            exclusions.append(
                InseeIndustrialProductionExcludedSearchResultV1(
                    document_id, title, subtitle, reason
                )
            )
    selected.sort(
        key=lambda item: _subtitle_period(
            str(item["sousTitre"]), cast(int, item["id"])
        )
    )
    search = InseeIndustrialProductionSearchV1(
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
) -> InseeIndustrialProductionReleaseV1:
    document_id = _integer(document.get("id"), "document id", 1_000_000_000)
    title = _text(document.get("titre"), "title")
    subtitle = _text(document.get("sousTitre"), "subtitle")
    period = _subtitle_period(subtitle, document_id)
    published = _timestamp(document.get("dateDiffusion"), "dateDiffusion")
    local = datetime.fromisoformat(published.replace("Z", "+00:00")).astimezone(
        ZoneInfo(INSEE_INDUSTRIAL_PRODUCTION_SOURCE_TIMEZONE)
    )
    parser = _parse_html(snapshot.content)
    page_text = _SPACE_RE.sub(" ", " ".join(parser.text)).strip()
    if title not in page_text and subtitle not in page_text:
        raise ValueError(
            f"INSEE industrial production page metadata differs: {document_id}"
        )
    return InseeIndustrialProductionReleaseV1(
        source_document_id=document_id,
        reference_period=period,
        title=title,
        subtitle=subtitle,
        published_at=published,
        published_at_local=local.isoformat(),
        source_uri=f"{INSEE_INDUSTRIAL_PRODUCTION_RELEASE_URI_PREFIX}{document_id}",
        artifact=_artifact(
            snapshot, InseeIndustrialProductionArtifactRole.RELEASE_HTML
        ),
        values=_release_values(parser, period, document_id),
    )


def _parse_catalog(
    snapshots: Sequence[OfficialRawSnapshotV1],
) -> tuple[InseeIndustrialProductionArtifactV1, ...]:
    retained = tuple(snapshots)
    if len(retained) != len(_CATALOG_BODIES):
        raise ValueError(
            "INSEE industrial production catalog inventory differs"
        )
    documents: list[Mapping[str, Any]] = []
    artifacts: list[InseeIndustrialProductionArtifactV1] = []
    expected_total: int | None = None
    for snapshot, body in zip(retained, _CATALOG_BODIES, strict=True):
        if (
            snapshot.request.method is not OfficialRequestMethod.POST
            or snapshot.request.body_text != _body(body)
        ):
            raise ValueError(
                "INSEE industrial production catalog request differs"
            )
        try:
            payload = json.loads(snapshot.content)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "INSEE industrial production catalog JSON is invalid"
            ) from exc
        root = _mapping(payload, "catalog")
        total = _integer(
            root.get("numFounds"),
            "numFounds",
            MAX_INSEE_INDUSTRIAL_PRODUCTION_RESULTS,
        )
        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise ValueError(
                "INSEE industrial production catalog page totals differ"
            )
        page = _sequence(root.get("documents"), "documents")
        expected_page_count = min(
            1000, max(0, total - cast(int, body["start"]))
        )
        if len(page) != expected_page_count:
            raise ValueError(
                "INSEE industrial production catalog page is truncated"
            )
        documents.extend(_mapping(item, "catalog document") for item in page)
        artifacts.append(
            _artifact(
                snapshot,
                InseeIndustrialProductionArtifactRole.SERIES_CATALOG,
            )
        )
    if expected_total != 1092 or len(documents) != expected_total:
        raise ValueError(
            "INSEE industrial production catalog result count differs"
        )
    by_id = {str(item.get("idBank")): item for item in documents}
    if len(by_id) != expected_total:
        raise ValueError("INSEE industrial production catalog IDs repeat")
    expected_titles = {
        INSEE_INDUSTRIAL_PRODUCTION_INDUSTRY_SERIES_ID: "SA-WDA industrial production index (base 100 in 2021) - "
        "Manufacturing, mining and quarrying and other industrial "
        "activities (NAF rev. 2, level A10, item BE)",
        INSEE_INDUSTRIAL_PRODUCTION_MANUFACTURING_SERIES_ID: "SA-WDA industrial production index (base 100 in 2021) - "
        "Manufacturing (NAF rev. 2, level Section, item C)",
    }
    for series_id, title in expected_titles.items():
        document = by_id.get(series_id)
        if (
            document is None
            or _text(document.get("titre"), "catalog title") != title
        ):
            raise ValueError(
                "INSEE industrial production catalog series identity differs"
            )
    return tuple(artifacts)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_current_series(
    snapshot: OfficialRawSnapshotV1,
) -> InseeIndustrialProductionCurrentSeriesV1:
    try:
        root = ET.fromstring(snapshot.content)
    except ET.ParseError as exc:
        raise ValueError(
            "INSEE industrial production SDMX XML is invalid"
        ) from exc
    matches = [
        item for item in root.iter() if _local_name(item.tag) == "Series"
    ]
    if len(matches) != 1:
        raise ValueError(
            "INSEE industrial production SDMX series identity differs"
        )
    series = matches[0]
    series_id = str(series.attrib.get("IDBANK", ""))
    definitions = {
        INSEE_INDUSTRIAL_PRODUCTION_INDUSTRY_SERIES_ID: (
            InseeIndustrialProductionAggregate.INDUSTRY,
            (
                "SA-WDA industrial production index (base 100 in 2021) - "
                "Manufacturing, mining and quarrying and other industrial "
                "activities (NAF rev. 2, level A10, item BE)"
            ),
        ),
        INSEE_INDUSTRIAL_PRODUCTION_MANUFACTURING_SERIES_ID: (
            InseeIndustrialProductionAggregate.MANUFACTURING,
            (
                "SA-WDA industrial production index (base 100 in 2021) - "
                "Manufacturing (NAF rev. 2, level Section, item C)"
            ),
        ),
    }
    definition = definitions.get(series_id)
    if (
        definition is None
        or series.attrib.get("FREQ") != "M"
        or series.attrib.get("UNIT_MEASURE") != "SO"
        or series.attrib.get("UNIT_MULT") != "0"
        or series.attrib.get("REF_AREA") != "FM"
        or series.attrib.get("TITLE_EN") != definition[1]
    ):
        raise ValueError(
            "INSEE industrial production SDMX series semantics differ"
        )
    observations = []
    for item in series:
        if _local_name(item.tag) != "Obs":
            continue
        lexical, numeric = _numeric(item.attrib.get("OBS_VALUE"), "OBS_VALUE")
        observations.append(
            InseeIndustrialProductionCurrentObservationV1(
                reference_period=_month(
                    item.attrib.get("TIME_PERIOD"), "TIME_PERIOD"
                ),
                lexical_value=lexical,
                value=numeric,
                status=str(item.attrib.get("OBS_STATUS", "<missing>")),
            )
        )
    observations.sort(key=lambda item: _month_ordinal(item.reference_period))
    return InseeIndustrialProductionCurrentSeriesV1(
        aggregate=definition[0],
        series_id=series_id,
        title=_text(series.attrib.get("TITLE_EN"), "TITLE_EN"),
        last_update=_iso_date(series.attrib.get("LAST_UPDATE"), "LAST_UPDATE"),
        observations=tuple(observations),
        artifact=_artifact(
            snapshot, InseeIndustrialProductionArtifactRole.CURRENT_SDMX
        ),
    )


_LIMITATIONS: Final = (
    "The qualified English monthly release-page lineage begins at 2009-04; document 1562488 is assigned to 2010-12 from its title, body, and publication chronology because its source subtitle incorrectly says December 2011.",
    "Document 2011499 preserves the source's French spelling 'Novembre' in its English subtitle while being normalized to reference month 2015-11.",
    "Document 3560105 has two malformed April 2018 comparison headers; its current-month column is bound to the page's title, introduction, row order, and repeated detailed table values.",
    "Release tables preserve source-authored SA-WDA month-over-month changes for whole industry (BE) and manufacturing (CZ) across 2005-, 2010-, 2015-, and 2021-base presentation changes.",
    "BDM observations are latest-revised and backcast index levels; comparisons derive current month-over-month growth from adjacent levels and never replace a historical release vintage.",
    "No official historical market-consensus series was identified, so surprise values are not manufactured.",
)


@dataclass(frozen=True, slots=True)
class InseeIndustrialProductionArchiveManifestV1:
    """Complete replayable INSEE monthly industrial-production manifest."""

    registry_id: str
    source_id: str
    search: InseeIndustrialProductionSearchV1
    releases: tuple[InseeIndustrialProductionReleaseV1, ...]
    catalog_artifacts: tuple[InseeIndustrialProductionArtifactV1, ...]
    current_series: tuple[InseeIndustrialProductionCurrentSeriesV1, ...]
    comparisons: tuple[InseeIndustrialProductionComparisonV1, ...]
    coverage_gaps: tuple[str, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    released_value_count: int
    compatible_comparison_count: int
    limitations: tuple[str, ...] = _LIMITATIONS
    source_key: str = INSEE_INDUSTRIAL_PRODUCTION_SOURCE_KEY
    program_key: str = INSEE_INDUSTRIAL_PRODUCTION_PROGRAM_KEY
    parser_id: str = INSEE_INDUSTRIAL_PRODUCTION_PARSER_ID
    parser_version: str = INSEE_INDUSTRIAL_PRODUCTION_PARSER_VERSION
    schema_version: str = INSEE_INDUSTRIAL_PRODUCTION_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_INDUSTRIAL_PRODUCTION_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE industrial production manifest schema"
            )
        if (
            self.source_key,
            self.program_key,
            self.parser_id,
            self.parser_version,
        ) != (
            INSEE_INDUSTRIAL_PRODUCTION_SOURCE_KEY,
            INSEE_INDUSTRIAL_PRODUCTION_PROGRAM_KEY,
            INSEE_INDUSTRIAL_PRODUCTION_PARSER_ID,
            INSEE_INDUSTRIAL_PRODUCTION_PARSER_VERSION,
        ):
            raise ValueError(
                "INSEE industrial production manifest program metadata differs"
            )
        for value, prefix, name in (
            (self.registry_id, "official-source-registry", "registry_id"),
            (self.source_id, "official-source", "source_id"),
        ):
            if re.fullmatch(rf"{prefix}:sha256:[0-9a-f]{{64}}", value) is None:
                raise ValueError(
                    f"INSEE industrial production manifest {name} is invalid"
                )
        releases = tuple(self.releases)
        if (
            tuple(item.reference_period for item in releases)
            != _release_periods()
        ):
            raise ValueError(
                "INSEE industrial production release coverage differs"
            )
        if tuple(item.source_document_id for item in releases) != (
            self.search.selected_document_ids
        ):
            raise ValueError(
                "INSEE industrial production release/search order differs"
            )
        if (
            tuple(self.coverage_gaps)
            != INSEE_INDUSTRIAL_PRODUCTION_RELEASE_GAPS
        ):
            raise ValueError(
                "INSEE industrial production source-gap accounting differs"
            )
        if len(self.catalog_artifacts) != len(_CATALOG_BODIES) or any(
            item.role
            is not InseeIndustrialProductionArtifactRole.SERIES_CATALOG
            for item in self.catalog_artifacts
        ):
            raise ValueError(
                "INSEE industrial production catalog artifact role differs"
            )
        if tuple(item.aggregate for item in self.current_series) != tuple(
            InseeIndustrialProductionAggregate
        ):
            raise ValueError(
                "INSEE industrial production current-series inventory differs"
            )
        expected_comparisons = tuple(
            (release.reference_period, value.aggregate)
            for release in releases
            for value in release.values
        )
        if (
            tuple(
                (item.reference_period, item.aggregate)
                for item in self.comparisons
            )
            != expected_comparisons
        ):
            raise ValueError(
                "INSEE industrial production exact-scope comparisons differ"
            )
        levels = {
            series.aggregate: {
                item.reference_period: item.value
                for item in series.observations
            }
            for series in self.current_series
        }
        for release, comparisons in zip(
            releases,
            (
                self.comparisons[index : index + 2]
                for index in range(0, len(self.comparisons), 2)
            ),
            strict=True,
        ):
            ordinal = _month_ordinal(release.reference_period) - 1
            previous = f"{ordinal // 12:04d}-{ordinal % 12 + 1:02d}"
            for published_value, comparison in zip(
                release.values, comparisons, strict=True
            ):
                current = levels[published_value.aggregate]
                expected_growth = round(
                    (
                        current[release.reference_period] / current[previous]
                        - 1.0
                    )
                    * 100.0,
                    12,
                )
                if (
                    comparison.release_value != published_value.value
                    or comparison.current_value != expected_growth
                ):
                    raise ValueError(
                        "INSEE industrial production comparison inputs differ"
                    )
        if any(
            item.published_at[:10] > self.search.as_of_date for item in releases
        ) or any(
            item.last_update > self.search.as_of_date
            for item in self.current_series
        ):
            raise ValueError(
                "INSEE industrial production evidence exceeds as-of date"
            )
        artifacts = (
            *self.search.artifacts,
            *(item.artifact for item in releases),
            *self.catalog_artifacts,
            *(item.artifact for item in self.current_series),
        )
        if self.raw_artifact_count != len(artifacts):
            raise ValueError(
                "INSEE industrial production raw artifact count differs"
            )
        if self.unique_content_sha256_count != len(
            {item.content_sha256 for item in artifacts}
        ):
            raise ValueError(
                "INSEE industrial production unique artifact count differs"
            )
        if self.total_content_bytes != sum(
            item.content_length for item in artifacts
        ):
            raise ValueError(
                "INSEE industrial production total content bytes differ"
            )
        if (
            not 0
            < self.total_content_bytes
            <= MAX_INSEE_INDUSTRIAL_PRODUCTION_TOTAL_BYTES
        ):
            raise ValueError(
                "INSEE industrial production total content bytes are outside bounds"
            )
        if self.released_value_count != sum(
            len(item.values) for item in releases
        ):
            raise ValueError(
                "INSEE industrial production released value count differs"
            )
        if self.compatible_comparison_count != len(self.comparisons):
            raise ValueError(
                "INSEE industrial production comparison count differs"
            )
        if tuple(self.limitations) != _LIMITATIONS:
            raise ValueError("INSEE industrial production limitations differ")

    @property
    def manifest_id(self) -> str:
        return _stable_id(
            "insee-industrial-production-archive-manifest",
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
            "catalog_artifacts": [
                item.to_dict() for item in self.catalog_artifacts
            ],
            "current_series": [item.to_dict() for item in self.current_series],
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
    ) -> InseeIndustrialProductionArchiveManifestV1:
        value = cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            search=InseeIndustrialProductionSearchV1.from_dict(
                _mapping(data.get("search"), "search")
            ),
            releases=tuple(
                InseeIndustrialProductionReleaseV1.from_dict(
                    _mapping(item, "release")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            catalog_artifacts=tuple(
                InseeIndustrialProductionArtifactV1.from_dict(
                    _mapping(item, "catalog artifact")
                )
                for item in _sequence(
                    data.get("catalog_artifacts"), "catalog_artifacts"
                )
            ),
            current_series=tuple(
                InseeIndustrialProductionCurrentSeriesV1.from_dict(
                    _mapping(item, "current series")
                )
                for item in _sequence(
                    data.get("current_series"), "current_series"
                )
            ),
            comparisons=tuple(
                InseeIndustrialProductionComparisonV1.from_dict(
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
            raise ValueError(
                "INSEE industrial production manifest identity differs"
            )
        return value

    @classmethod
    def from_json(
        cls, payload: str
    ) -> InseeIndustrialProductionArchiveManifestV1:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "INSEE industrial production manifest JSON is invalid"
            ) from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_insee_industrial_production_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search: InseeIndustrialProductionSearchV1,
    documents: Sequence[Mapping[str, Any]],
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    catalog_snapshots: Sequence[OfficialRawSnapshotV1],
    current_series_snapshots: Sequence[OfficialRawSnapshotV1],
) -> InseeIndustrialProductionArchiveManifestV1:
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
            "INSEE industrial production retained release inventory differs"
        )
    request_groups: tuple[
        tuple[
            Sequence[
                OfficialRawSnapshotV1 | InseeIndustrialProductionArtifactV1
            ],
            tuple[OfficialSourceRequestV1, ...],
        ],
        ...,
    ] = (
        (
            search.artifacts,
            build_insee_industrial_production_search_requests(registry),
        ),
        (
            tuple(release_snapshots[item] for item in ids),
            build_insee_industrial_production_release_requests(
                registry, documents
            ),
        ),
        (
            tuple(catalog_snapshots),
            build_insee_industrial_production_catalog_requests(registry),
        ),
        (
            tuple(current_series_snapshots),
            build_insee_industrial_production_current_series_requests(registry),
        ),
    )
    for retained, expected in request_groups:
        if len(retained) != len(expected):
            raise ValueError(
                "INSEE industrial production request inventory differs"
            )
        for item, request in zip(retained, expected, strict=True):
            if isinstance(item, OfficialRawSnapshotV1):
                if item.request != request or item.resolved_uri != request.uri:
                    raise ValueError(
                        "INSEE industrial production snapshot request differs"
                    )
            elif item.request_id != request.request_id:
                raise ValueError(
                    "INSEE industrial production search request identity differs"
                )
    releases = tuple(
        _parse_release(document, release_snapshots[document_id])
        for document, document_id in zip(documents, ids, strict=True)
    )
    catalog_artifacts = _parse_catalog(catalog_snapshots)
    current_series = tuple(
        _parse_current_series(snapshot) for snapshot in current_series_snapshots
    )
    current_series = tuple(
        sorted(
            current_series,
            key=lambda item: tuple(InseeIndustrialProductionAggregate).index(
                item.aggregate
            ),
        )
    )
    if tuple(item.aggregate for item in current_series) != tuple(
        InseeIndustrialProductionAggregate
    ):
        raise ValueError(
            "INSEE industrial production current-series snapshots differ"
        )
    current_by_aggregate = {
        series.aggregate: {
            item.reference_period: item.value for item in series.observations
        }
        for series in current_series
    }
    comparisons: list[InseeIndustrialProductionComparisonV1] = []
    for release in releases:
        ordinal = _month_ordinal(release.reference_period) - 1
        previous = f"{ordinal // 12:04d}-{ordinal % 12 + 1:02d}"
        for value in release.values:
            levels = current_by_aggregate[value.aggregate]
            current_growth = round(
                (levels[release.reference_period] / levels[previous] - 1.0)
                * 100.0,
                12,
            )
            comparisons.append(
                InseeIndustrialProductionComparisonV1(
                    aggregate=value.aggregate,
                    reference_period=release.reference_period,
                    release_value=value.value,
                    current_value=current_growth,
                    delta=round(current_growth - value.value, 12),
                )
            )
    artifacts = (
        *search.artifacts,
        *(item.artifact for item in releases),
        *catalog_artifacts,
        *(item.artifact for item in current_series),
    )
    return InseeIndustrialProductionArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        search=search,
        releases=releases,
        catalog_artifacts=catalog_artifacts,
        current_series=current_series,
        comparisons=tuple(comparisons),
        coverage_gaps=INSEE_INDUSTRIAL_PRODUCTION_RELEASE_GAPS,
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
        released_value_count=sum(len(item.values) for item in releases),
        compatible_comparison_count=len(comparisons),
    )


def replay_insee_industrial_production_archive(
    registry: OfficialSourceRegistryV1,
    manifest: InseeIndustrialProductionArchiveManifestV1,
    search_snapshots: Sequence[OfficialRawSnapshotV1],
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    catalog_snapshots: Sequence[OfficialRawSnapshotV1],
    current_series_snapshots: Sequence[OfficialRawSnapshotV1],
) -> InseeIndustrialProductionArchiveManifestV1:
    """Rebuild retained evidence and require byte-independent equality."""
    if registry.registry_id != manifest.registry_id:
        raise ValueError(
            "INSEE industrial production replay registry identity differs"
        )
    search, documents = parse_insee_industrial_production_search(
        search_snapshots, as_of_date=manifest.search.as_of_date
    )
    rebuilt = build_insee_industrial_production_archive_manifest(
        registry,
        search,
        documents,
        release_snapshots,
        catalog_snapshots,
        current_series_snapshots,
    )
    if rebuilt != manifest:
        raise ValueError(
            "INSEE industrial production replay differs from retained manifest"
        )
    return rebuilt


def packaged_insee_industrial_production_manifest_path() -> Path:
    """Return the packaged INSEE monthly industrial-production manifest path."""
    return (
        Path(__file__).with_name("assets")
        / "insee_industrial_production_archive_v1.json"
    )


def load_packaged_insee_industrial_production_archive_manifest() -> (
    InseeIndustrialProductionArchiveManifestV1
):
    """Load and fully validate the packaged archive."""
    path = packaged_insee_industrial_production_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError(
            "packaged INSEE industrial production manifest exceeds size bound"
        )
    return InseeIndustrialProductionArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )


__all__ = [
    "INSEE_INDUSTRIAL_PRODUCTION_CATALOG_URI",
    "INSEE_INDUSTRIAL_PRODUCTION_CURRENT_SERIES_IDS",
    "INSEE_INDUSTRIAL_PRODUCTION_CURRENT_SERIES_URIS",
    "INSEE_INDUSTRIAL_PRODUCTION_CURRENT_START_PERIOD",
    "INSEE_INDUSTRIAL_PRODUCTION_INDUSTRY_SERIES_ID",
    "INSEE_INDUSTRIAL_PRODUCTION_LATEST_PACKAGED_PERIOD",
    "INSEE_INDUSTRIAL_PRODUCTION_MANUFACTURING_SERIES_ID",
    "INSEE_INDUSTRIAL_PRODUCTION_PARSER_ID",
    "INSEE_INDUSTRIAL_PRODUCTION_PARSER_VERSION",
    "INSEE_INDUSTRIAL_PRODUCTION_PROGRAM_KEY",
    "INSEE_INDUSTRIAL_PRODUCTION_RELEASE_GAPS",
    "INSEE_INDUSTRIAL_PRODUCTION_RELEASE_START_PERIOD",
    "INSEE_INDUSTRIAL_PRODUCTION_SEARCH_URI",
    "INSEE_INDUSTRIAL_PRODUCTION_SOURCE_KEY",
    "InseeIndustrialProductionAggregate",
    "InseeIndustrialProductionArchiveManifestV1",
    "InseeIndustrialProductionArtifactRole",
    "InseeIndustrialProductionArtifactV1",
    "InseeIndustrialProductionComparisonV1",
    "InseeIndustrialProductionCurrentObservationV1",
    "InseeIndustrialProductionCurrentSeriesV1",
    "InseeIndustrialProductionExcludedSearchResultV1",
    "InseeIndustrialProductionPublishedValueV1",
    "InseeIndustrialProductionReleaseV1",
    "InseeIndustrialProductionSearchV1",
    "build_insee_industrial_production_archive_manifest",
    "build_insee_industrial_production_catalog_requests",
    "build_insee_industrial_production_current_series_requests",
    "build_insee_industrial_production_release_requests",
    "build_insee_industrial_production_search_requests",
    "load_packaged_insee_industrial_production_archive_manifest",
    "packaged_insee_industrial_production_manifest_path",
    "parse_insee_industrial_production_search",
    "replay_insee_industrial_production_archive",
]
