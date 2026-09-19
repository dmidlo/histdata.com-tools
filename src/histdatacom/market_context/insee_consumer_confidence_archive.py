"""Deterministic qualification of INSEE monthly consumer-confidence evidence.

The archive retains one bounded English Solr inventory, every selected
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

INSEE_CONSUMER_CONFIDENCE_SOURCE_KEY: Final = "fr.insee.consumer-confidence"
INSEE_CONSUMER_CONFIDENCE_PROGRAM_KEY: Final = "fr.insee.consumer-confidence"
INSEE_CONSUMER_CONFIDENCE_PARSER_ID: Final = (
    "official.insee-consumer-confidence.v1"
)
INSEE_CONSUMER_CONFIDENCE_PARSER_VERSION: Final = "1"
INSEE_CONSUMER_CONFIDENCE_SOURCE_TIMEZONE: Final = "Europe/Paris"
INSEE_CONSUMER_CONFIDENCE_SEARCH_URI: Final = (
    "https://www.insee.fr/en/solr/consultation"
)
INSEE_CONSUMER_CONFIDENCE_CATALOG_URI: Final = (
    "https://www.insee.fr/en/statistiques/series/ajax/consultation"
)
INSEE_CONSUMER_CONFIDENCE_RELEASE_URI_PREFIX: Final = (
    "https://www.insee.fr/en/statistiques/"
)
INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_ID: Final = "001587668"
INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_URI: Final = (
    "https://bdm.insee.fr/series/sdmx/data/SERIES_BDM/001587668"
    "?startPeriod=2000-01&endPeriod=2026-08"
)
INSEE_CONSUMER_CONFIDENCE_RELEASE_START_PERIOD: Final = "2009-06"
INSEE_CONSUMER_CONFIDENCE_CURRENT_START_PERIOD: Final = "2000-01"
INSEE_CONSUMER_CONFIDENCE_LATEST_PACKAGED_PERIOD: Final = "2026-08"
INSEE_CONSUMER_CONFIDENCE_RELEASE_GAPS: Final = (
    "2009-08",
    "2010-08",
    "2011-08",
    "2012-08",
)
INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_PERIOD: Final = "2023-01"
INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_DOCUMENT_ID: Final = 6_794_004
INSEE_CONSUMER_CONFIDENCE_REPLACEMENT_URI: Final = (
    "https://www.insee.fr/en/statistiques/6958383"
)

INSEE_CONSUMER_CONFIDENCE_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.insee-consumer-confidence-artifact.v1"
)
INSEE_CONSUMER_CONFIDENCE_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.insee-consumer-confidence-exclusion.v1"
)
INSEE_CONSUMER_CONFIDENCE_SEARCH_SCHEMA_VERSION: Final = (
    "histdatacom.insee-consumer-confidence-search.v1"
)
INSEE_CONSUMER_CONFIDENCE_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.insee-consumer-confidence-value.v1"
)
INSEE_CONSUMER_CONFIDENCE_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.insee-consumer-confidence-release.v1"
)
INSEE_CONSUMER_CONFIDENCE_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.insee-consumer-confidence-current-observation.v1"
)
INSEE_CONSUMER_CONFIDENCE_SERIES_SCHEMA_VERSION: Final = (
    "histdatacom.insee-consumer-confidence-current-series.v1"
)
INSEE_CONSUMER_CONFIDENCE_COMPARISON_SCHEMA_VERSION: Final = (
    "histdatacom.insee-consumer-confidence-comparison.v1"
)
INSEE_CONSUMER_CONFIDENCE_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.insee-consumer-confidence-archive-manifest.v1"
)

MAX_INSEE_CONSUMER_CONFIDENCE_RESULTS: Final = 1_000
MAX_INSEE_CONSUMER_CONFIDENCE_ARTIFACT_BYTES: Final = 64 * 1024 * 1024
MAX_INSEE_CONSUMER_CONFIDENCE_TOTAL_BYTES: Final = 512 * 1024 * 1024
_SPACE_RE = re.compile(r"\s+")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMBER_RE = re.compile(r"^[+\-]?\d+(?:[.,]\d+)?$")
_MONTH_RE = re.compile(r"^(?P<year>\d{4})-(?P<month>0[1-9]|1[0-2])$")
_SUBTITLE_RE = re.compile(
    r"^Monthly consumer confidence survey - "
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
_SEARCH_BODY: Final[Mapping[str, JSONValue]] = {
    "facetsQuery": [],
    "filters": [
        {"field": "diffusion", "values": [True]},
        {"field": "rubrique", "values": ["statistiques"]},
    ],
    "q": "consumer confidence",
    "rows": 1000,
    "sortFields": [{"field": "dateDiffusion", "order": "desc"}],
    "start": 0,
}
_CATALOG_BODY: Final[Mapping[str, JSONValue]] = {
    "facetsField": [],
    "filters": [{"field": "bdm_idFamille", "values": ["102414547"]}],
    "q": "summary indicator",
    "rows": 1000,
    "sortFields": [],
    "start": 0,
}
_METHODOLOGIES: Final = {
    "arithmetic-mean",
    "factor-analysis",
    "normalized-synthetic-index",
    "source-unspecified",
}


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
    gaps = set(INSEE_CONSUMER_CONFIDENCE_RELEASE_GAPS)
    return tuple(
        period
        for period in _month_range(
            INSEE_CONSUMER_CONFIDENCE_RELEASE_START_PERIOD,
            INSEE_CONSUMER_CONFIDENCE_LATEST_PACKAGED_PERIOD,
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
    source = registry.source(INSEE_CONSUMER_CONFIDENCE_SOURCE_KEY)
    if (
        source.parser_id != INSEE_CONSUMER_CONFIDENCE_PARSER_ID
        or source.parser_version != INSEE_CONSUMER_CONFIDENCE_PARSER_VERSION
    ):
        raise ValueError(
            "INSEE consumer confidence registry parser binding differs"
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
            "INSEE consumer confidence request host differs from registry"
        )
    if source_format not in source.formats:
        raise ValueError(
            "INSEE consumer confidence request format differs from registry"
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


def build_insee_consumer_confidence_search_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact bounded Solr request used for release discovery."""
    return _request(
        registry,
        INSEE_CONSUMER_CONFIDENCE_SEARCH_URI,
        OfficialSourceFormat.JSON,
        method=OfficialRequestMethod.POST,
        body=_SEARCH_BODY,
    )


def build_insee_consumer_confidence_release_requests(
    registry: OfficialSourceRegistryV1,
    documents: Sequence[Mapping[str, Any]],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact HTML request per selected release document."""
    return tuple(
        _request(
            registry,
            f"{INSEE_CONSUMER_CONFIDENCE_RELEASE_URI_PREFIX}"
            f"{_integer(item.get('id'), 'document id', 1_000_000_000)}",
            OfficialSourceFormat.HTML,
        )
        for item in documents
    )


def build_insee_consumer_confidence_catalog_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact series-catalog request proving the selected BDM ID."""
    return _request(
        registry,
        INSEE_CONSUMER_CONFIDENCE_CATALOG_URI,
        OfficialSourceFormat.JSON,
        method=OfficialRequestMethod.POST,
        body=_CATALOG_BODY,
    )


def build_insee_consumer_confidence_current_series_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the bounded latest-state BDM confidence-series request."""
    return _request(
        registry,
        INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_URI,
        OfficialSourceFormat.SDMX_21,
    )


class InseeConsumerConfidenceArtifactRole(str, Enum):
    """Semantic role of one retained official response."""

    RELEASE_SEARCH = "release-search"
    RELEASE_HTML = "release-html"
    SERIES_CATALOG = "series-catalog"
    CURRENT_SDMX = "current-sdmx"


@dataclass(frozen=True, slots=True)
class InseeConsumerConfidenceArtifactV1:
    """Hash-bound receipt for one exact official response."""

    role: InseeConsumerConfidenceArtifactRole
    source_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    request_id: str
    schema_version: str = INSEE_CONSUMER_CONFIDENCE_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_CONSUMER_CONFIDENCE_ARTIFACT_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE consumer confidence artifact schema"
            )
        if not isinstance(self.role, InseeConsumerConfidenceArtifactRole):
            raise TypeError(
                "INSEE consumer confidence artifact role is invalid"
            )
        if not isinstance(self.source_format, OfficialSourceFormat):
            raise TypeError(
                "INSEE consumer confidence artifact format is invalid"
            )
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        if _DIGEST_RE.fullmatch(self.content_sha256) is None:
            raise ValueError(
                "INSEE consumer confidence artifact digest is invalid"
            )
        if (
            not 0
            < self.content_length
            <= MAX_INSEE_CONSUMER_CONFIDENCE_ARTIFACT_BYTES
        ):
            raise ValueError(
                "INSEE consumer confidence artifact length is invalid"
            )
        if (
            re.fullmatch(
                r"official-request:sha256:[0-9a-f]{64}", self.request_id
            )
            is None
        ):
            raise ValueError(
                "INSEE consumer confidence request identity is invalid"
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
    ) -> InseeConsumerConfidenceArtifactV1:
        return cls(
            role=InseeConsumerConfidenceArtifactRole(str(data.get("role", ""))),
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
    snapshot: OfficialRawSnapshotV1, role: InseeConsumerConfidenceArtifactRole
) -> InseeConsumerConfidenceArtifactV1:
    if (
        snapshot.request.source_key != INSEE_CONSUMER_CONFIDENCE_SOURCE_KEY
        or snapshot.request.parser_id != INSEE_CONSUMER_CONFIDENCE_PARSER_ID
        or snapshot.request.parser_version
        != INSEE_CONSUMER_CONFIDENCE_PARSER_VERSION
    ):
        raise ValueError(
            "INSEE consumer confidence snapshot names another source"
        )
    return InseeConsumerConfidenceArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        request_id=snapshot.request.request_id,
    )


@dataclass(frozen=True, slots=True)
class InseeConsumerConfidenceExcludedSearchResultV1:
    """One explicitly classified but unselected Solr result."""

    source_document_id: int
    title: str
    subtitle: str | None
    reason: str
    schema_version: str = INSEE_CONSUMER_CONFIDENCE_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_CONSUMER_CONFIDENCE_EXCLUSION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE consumer confidence exclusion schema"
            )
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError(
                "INSEE consumer confidence excluded document ID is invalid"
            )
        object.__setattr__(self, "title", _text(self.title, "title"))
        if self.subtitle is not None:
            object.__setattr__(
                self, "subtitle", _text(self.subtitle, "subtitle")
            )
        if self.reason not in {
            "missing-subtitle",
            "outside-packaged-coverage",
            "other-consumer-confidence-product",
            "search-result-outside-release-lineage",
        }:
            raise ValueError(
                "INSEE consumer confidence exclusion reason is invalid"
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
    ) -> InseeConsumerConfidenceExcludedSearchResultV1:
        raw_subtitle = data.get("subtitle")
        return cls(
            source_document_id=cast(int, data.get("source_document_id")),
            title=str(data.get("title", "")),
            subtitle=None if raw_subtitle is None else str(raw_subtitle),
            reason=str(data.get("reason", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeConsumerConfidenceSearchV1:
    """Complete accounting of the bounded release-search response."""

    as_of_date: str
    artifact: InseeConsumerConfidenceArtifactV1
    total_result_count: int
    selected_document_ids: tuple[int, ...]
    exclusions: tuple[InseeConsumerConfidenceExcludedSearchResultV1, ...]
    schema_version: str = INSEE_CONSUMER_CONFIDENCE_SEARCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_CONSUMER_CONFIDENCE_SEARCH_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE consumer confidence search schema"
            )
        object.__setattr__(
            self, "as_of_date", _iso_date(self.as_of_date, "as_of_date")
        )
        if (
            self.artifact.role
            is not InseeConsumerConfidenceArtifactRole.RELEASE_SEARCH
        ):
            raise ValueError(
                "INSEE consumer confidence search artifact role differs"
            )
        if (
            not 0
            < self.total_result_count
            <= MAX_INSEE_CONSUMER_CONFIDENCE_RESULTS
        ):
            raise ValueError(
                "INSEE consumer confidence search result count is invalid"
            )
        ids = tuple(self.selected_document_ids)
        if len(ids) != len(set(ids)):
            raise ValueError(
                "INSEE consumer confidence selected IDs are not unique"
            )
        excluded_ids = {item.source_document_id for item in self.exclusions}
        if len(ids) + len(excluded_ids) != self.total_result_count:
            raise ValueError(
                "INSEE consumer confidence selected/excluded accounting differs"
            )
        if set(ids) & excluded_ids:
            raise ValueError(
                "INSEE consumer confidence selected and excluded IDs overlap"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "as_of_date": self.as_of_date,
            "artifact": self.artifact.to_dict(),
            "total_result_count": self.total_result_count,
            "selected_document_ids": list(self.selected_document_ids),
            "exclusions": [item.to_dict() for item in self.exclusions],
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeConsumerConfidenceSearchV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            artifact=InseeConsumerConfidenceArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            total_result_count=cast(int, data.get("total_result_count")),
            selected_document_ids=tuple(
                cast(int, item)
                for item in _sequence(
                    data.get("selected_document_ids"), "selected_document_ids"
                )
            ),
            exclusions=tuple(
                InseeConsumerConfidenceExcludedSearchResultV1.from_dict(
                    _mapping(item, "exclusion")
                )
                for item in _sequence(data.get("exclusions"), "exclusions")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeConsumerConfidencePublishedValueV1:
    """One contemporaneous household-confidence summary indicator."""

    lexical_value: str
    value: float
    locator: str
    methodology: str
    metric: str = "household-confidence-summary-indicator"
    unit: str = "normalized-index-long-term-average-100"
    adjustment: str = "seasonally-adjusted"
    geography_scope: str = "france"
    schema_version: str = INSEE_CONSUMER_CONFIDENCE_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_CONSUMER_CONFIDENCE_VALUE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE consumer confidence value schema"
            )
        lexical, parsed = _numeric(self.lexical_value, "lexical_value")
        object.__setattr__(self, "lexical_value", lexical)
        if not isinstance(self.value, (float, int)) or isinstance(
            self.value, bool
        ):
            raise TypeError("INSEE consumer confidence value must be numeric")
        if abs(float(self.value) - parsed) > 1e-12:
            raise ValueError(
                "INSEE consumer confidence lexical and numeric values differ"
            )
        object.__setattr__(self, "value", float(self.value))
        object.__setattr__(self, "locator", _text(self.locator, "locator"))
        if self.geography_scope != "france":
            raise ValueError(
                "INSEE consumer confidence geography scope differs"
            )
        if self.methodology not in _METHODOLOGIES:
            raise ValueError("INSEE consumer confidence methodology differs")
        if (
            self.metric,
            self.unit,
            self.adjustment,
        ) != (
            "household-confidence-summary-indicator",
            "normalized-index-long-term-average-100",
            "seasonally-adjusted",
        ):
            raise ValueError("INSEE consumer confidence value semantics differ")

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
    ) -> InseeConsumerConfidencePublishedValueV1:
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
            "INSEE consumer confidence subtitle is outside the selected lineage"
        )
    month = _MONTH_NUMBER.get(match.group("month").casefold())
    if month is None:
        raise ValueError("INSEE consumer confidence subtitle month differs")
    return f"{match.group('year')}-{month:02d}"


@dataclass(frozen=True, slots=True)
class InseeConsumerConfidenceReleaseV1:
    """One source-dated INSEE monthly consumer-confidence publication."""

    source_document_id: int
    reference_period: str
    title: str
    subtitle: str
    published_at: str
    published_at_local: str
    source_uri: str
    artifact: InseeConsumerConfidenceArtifactV1
    value: InseeConsumerConfidencePublishedValueV1 | None
    value_unavailable_reason: str | None = None
    replacement_source_uri: str | None = None
    schema_version: str = INSEE_CONSUMER_CONFIDENCE_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_CONSUMER_CONFIDENCE_RELEASE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE consumer confidence release schema"
            )
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError("INSEE consumer confidence document ID is invalid")
        object.__setattr__(
            self,
            "reference_period",
            _month(self.reference_period, "reference_period"),
        )
        if _subtitle_period(self.subtitle) != self.reference_period:
            raise ValueError(
                "INSEE consumer confidence subtitle period differs"
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
        ):
            raise ValueError(
                "INSEE consumer confidence local publication timestamp differs"
            )
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        expected_uri = f"{INSEE_CONSUMER_CONFIDENCE_RELEASE_URI_PREFIX}{self.source_document_id}"
        if (
            self.source_uri != expected_uri
            or self.artifact.source_uri != expected_uri
        ):
            raise ValueError("INSEE consumer confidence release URI differs")
        if (
            self.artifact.role
            is not InseeConsumerConfidenceArtifactRole.RELEASE_HTML
        ):
            raise ValueError(
                "INSEE consumer confidence release artifact role differs"
            )
        if self.value is None:
            if (
                self.reference_period,
                self.source_document_id,
                self.value_unavailable_reason,
                self.replacement_source_uri,
            ) != (
                INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_PERIOD,
                INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_DOCUMENT_ID,
                "withdrawn-after-significant-error",
                INSEE_CONSUMER_CONFIDENCE_REPLACEMENT_URI,
            ):
                raise ValueError(
                    "INSEE consumer confidence unavailable value differs"
                )
        elif (
            self.value_unavailable_reason is not None
            or self.replacement_source_uri is not None
        ):
            raise ValueError(
                "INSEE consumer confidence published value has withdrawal metadata"
            )

    @property
    def release_id(self) -> str:
        return _stable_id(
            "insee-consumer-confidence-release", self.identity_payload()
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
            "value": None if self.value is None else self.value.to_dict(),
            "value_unavailable_reason": self.value_unavailable_reason,
            "replacement_source_uri": self.replacement_source_uri,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "release_id": self.release_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeConsumerConfidenceReleaseV1:
        raw_value = data.get("value")
        value = cls(
            source_document_id=cast(int, data.get("source_document_id")),
            reference_period=str(data.get("reference_period", "")),
            title=str(data.get("title", "")),
            subtitle=str(data.get("subtitle", "")),
            published_at=str(data.get("published_at", "")),
            published_at_local=str(data.get("published_at_local", "")),
            source_uri=str(data.get("source_uri", "")),
            artifact=InseeConsumerConfidenceArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            value=(
                None
                if raw_value is None
                else InseeConsumerConfidencePublishedValueV1.from_dict(
                    _mapping(raw_value, "value")
                )
            ),
            value_unavailable_reason=(
                None
                if data.get("value_unavailable_reason") is None
                else str(data.get("value_unavailable_reason"))
            ),
            replacement_source_uri=(
                None
                if data.get("replacement_source_uri") is None
                else str(data.get("replacement_source_uri"))
            ),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("release_id") != value.release_id:
            raise ValueError(
                "INSEE consumer confidence release identity differs"
            )
        return value


@dataclass(frozen=True, slots=True)
class InseeConsumerConfidenceCurrentObservationV1:
    """One latest-revised observation from the BDM confidence series."""

    reference_period: str
    lexical_value: str
    value: float
    status: str
    schema_version: str = INSEE_CONSUMER_CONFIDENCE_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_CONSUMER_CONFIDENCE_OBSERVATION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE consumer confidence observation schema"
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
                "INSEE consumer confidence observation lexical differs"
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
    ) -> InseeConsumerConfidenceCurrentObservationV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            lexical_value=str(data.get("lexical_value", "")),
            value=float(cast(float, data.get("value"))),
            status=str(data.get("status", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeConsumerConfidenceCurrentSeriesV1:
    """Latest revised BDM series, explicitly separate from release vintages."""

    series_id: str
    title: str
    last_update: str
    observations: tuple[InseeConsumerConfidenceCurrentObservationV1, ...]
    artifact: InseeConsumerConfidenceArtifactV1
    metric: str = "household-confidence-summary-indicator"
    unit: str = "normalized-index-long-term-average-100"
    adjustment: str = "seasonally-adjusted"
    geography_scope: str = "france"
    methodology: str = "factor-analysis"
    semantics: str = "latest-revised-cross-check"
    schema_version: str = INSEE_CONSUMER_CONFIDENCE_SERIES_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_CONSUMER_CONFIDENCE_SERIES_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE consumer confidence series schema"
            )
        if self.series_id != INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_ID:
            raise ValueError(
                "INSEE consumer confidence current series identity differs"
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
            "household-confidence-summary-indicator",
            "normalized-index-long-term-average-100",
            "seasonally-adjusted",
            "france",
            "factor-analysis",
            "latest-revised-cross-check",
        ):
            raise ValueError(
                "INSEE consumer confidence current series semantics differ"
            )
        values = tuple(self.observations)
        expected = _month_range(
            INSEE_CONSUMER_CONFIDENCE_CURRENT_START_PERIOD,
            INSEE_CONSUMER_CONFIDENCE_LATEST_PACKAGED_PERIOD,
        )
        if tuple(item.reference_period for item in values) != expected:
            raise ValueError(
                "INSEE consumer confidence current-series coverage differs"
            )
        if (
            self.artifact.role
            is not InseeConsumerConfidenceArtifactRole.CURRENT_SDMX
        ):
            raise ValueError(
                "INSEE consumer confidence current-series artifact role differs"
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
    ) -> InseeConsumerConfidenceCurrentSeriesV1:
        return cls(
            series_id=str(data.get("series_id", "")),
            title=str(data.get("title", "")),
            last_update=str(data.get("last_update", "")),
            observations=tuple(
                InseeConsumerConfidenceCurrentObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            artifact=InseeConsumerConfidenceArtifactV1.from_dict(
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
class InseeConsumerConfidenceComparisonV1:
    """Exact-scope release versus latest-revised BDM comparison."""

    reference_period: str
    release_value: float
    current_value: float
    delta: float
    methodology: str = "factor-analysis"
    schema_version: str = INSEE_CONSUMER_CONFIDENCE_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_CONSUMER_CONFIDENCE_COMPARISON_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE consumer confidence comparison schema"
            )
        object.__setattr__(
            self,
            "reference_period",
            _month(self.reference_period, "reference_period"),
        )
        if self.methodology != "factor-analysis":
            raise ValueError(
                "INSEE consumer confidence comparison method differs"
            )
        expected = round(
            float(self.current_value) - float(self.release_value), 12
        )
        if abs(float(self.delta) - expected) > 1e-12:
            raise ValueError(
                "INSEE consumer confidence comparison delta differs"
            )
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
    ) -> InseeConsumerConfidenceComparisonV1:
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
        raise ValueError(
            "INSEE consumer confidence release is not UTF-8"
        ) from exc
    parser = _ReleaseHtmlParser()
    parser.feed(source)
    parser.close()
    return parser


def _release_methodology(parser: _ReleaseHtmlParser) -> str:
    page_text = _SPACE_RE.sub(" ", " ".join(parser.text)).casefold()
    if "arithmetic mean" in page_text:
        return "arithmetic-mean"
    if "factor analysis" in page_text or "first factor" in page_text:
        return "factor-analysis"
    if "synthetic index" in page_text or "summary indicator" in page_text:
        return "normalized-synthetic-index"
    return "source-unspecified"


def _release_value(
    parser: _ReleaseHtmlParser, period: str
) -> InseeConsumerConfidencePublishedValueV1:
    methodology = _release_methodology(parser)
    for table_index, table in enumerate(parser.tables):
        if not table.rows:
            continue
        header = tuple(cell.casefold() for cell in table.rows[0])
        confidence_columns = tuple(
            index
            for index, cell in enumerate(header)
            if "household confidence synthetic index" in cell
        )
        if confidence_columns:
            column = confidence_columns[0]
            for row_index, row in enumerate(table.rows[1:], 1):
                if row and row[0] == period and column < len(row):
                    lexical, numeric = _numeric(
                        row[column], "household-confidence chart value"
                    )
                    return InseeConsumerConfidencePublishedValueV1(
                        lexical_value=lexical,
                        value=numeric,
                        methodology=methodology,
                        locator=(
                            f"table:{table_index}:{table.table_id}:"
                            f"row:{row_index}:column:{column}"
                        ),
                    )
        for row_index, row in enumerate(table.rows):
            if len(row) < 2:
                continue
            label = row[0].casefold().rstrip("* ")
            if not (
                label == "summary indicator"
                or label.startswith(
                    (
                        "synthetic index",
                        "composite indicator",
                        "household confidence synthetic index",
                    )
                )
            ):
                continue
            for column in range(len(row) - 1, 0, -1):
                try:
                    lexical, numeric = _numeric(
                        row[column], "household-confidence table value"
                    )
                except ValueError:
                    continue
                return InseeConsumerConfidencePublishedValueV1(
                    lexical_value=lexical,
                    value=numeric,
                    methodology=methodology,
                    locator=(
                        f"table:{table_index}:{table.table_id}:"
                        f"row:{row_index}:column:{column}"
                    ),
                )
    raise ValueError(
        f"INSEE consumer confidence release omits its summary value for {period}"
    )


def parse_insee_consumer_confidence_search(
    snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> tuple[InseeConsumerConfidenceSearchV1, tuple[Mapping[str, Any], ...]]:
    """Classify every Solr result and retain the monthly release lineage."""
    boundary = _iso_date(as_of_date, "as_of_date")
    if (
        snapshot.request.method is not OfficialRequestMethod.POST
        or snapshot.request.body_text != _body(_SEARCH_BODY)
    ):
        raise ValueError("INSEE consumer confidence search request differs")
    try:
        payload = json.loads(snapshot.content)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "INSEE consumer confidence search JSON is invalid"
        ) from exc
    root = _mapping(payload, "search")
    documents = tuple(
        _mapping(item, "search document")
        for item in _sequence(root.get("documents"), "documents")
    )
    total = _integer(
        root.get("numFounds"),
        "numFounds",
        MAX_INSEE_CONSUMER_CONFIDENCE_RESULTS,
    )
    if total != len(documents):
        raise ValueError("INSEE consumer confidence Solr response is truncated")
    selected: list[Mapping[str, Any]] = []
    exclusions: list[InseeConsumerConfidenceExcludedSearchResultV1] = []
    seen: set[int] = set()
    for document in documents:
        document_id = _integer(document.get("id"), "document id", 1_000_000_000)
        if document_id in seen:
            raise ValueError(
                "INSEE consumer confidence Solr response repeats a document"
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
        elif "confidence" in f"{title} {subtitle or ''}".casefold():
            reason = "other-consumer-confidence-product"
        else:
            reason = "search-result-outside-release-lineage"
        exclusions.append(
            InseeConsumerConfidenceExcludedSearchResultV1(
                document_id, title, subtitle, reason
            )
        )
    selected.sort(key=lambda item: _subtitle_period(str(item["sousTitre"])))
    search = InseeConsumerConfidenceSearchV1(
        as_of_date=boundary,
        artifact=_artifact(
            snapshot, InseeConsumerConfidenceArtifactRole.RELEASE_SEARCH
        ),
        total_result_count=total,
        selected_document_ids=tuple(cast(int, item["id"]) for item in selected),
        exclusions=tuple(
            sorted(exclusions, key=lambda item: item.source_document_id)
        ),
    )
    return search, tuple(selected)


def _parse_release(
    document: Mapping[str, Any], snapshot: OfficialRawSnapshotV1
) -> InseeConsumerConfidenceReleaseV1:
    document_id = _integer(document.get("id"), "document id", 1_000_000_000)
    title = _text(document.get("titre"), "title")
    subtitle = _text(document.get("sousTitre"), "subtitle")
    period = _subtitle_period(subtitle)
    published = _timestamp(document.get("dateDiffusion"), "dateDiffusion")
    local = datetime.fromisoformat(published.replace("Z", "+00:00")).astimezone(
        ZoneInfo(INSEE_CONSUMER_CONFIDENCE_SOURCE_TIMEZONE)
    )
    parser = _parse_html(snapshot.content)
    page_text = _SPACE_RE.sub(" ", " ".join(parser.text)).strip()
    if title not in page_text and subtitle not in page_text:
        raise ValueError(
            f"INSEE consumer confidence page metadata differs: {document_id}"
        )
    value: InseeConsumerConfidencePublishedValueV1 | None
    unavailable_reason: str | None = None
    replacement_uri: str | None = None
    try:
        value = _release_value(parser, period)
    except ValueError:
        normalized_page_text = page_text.casefold()
        if not (
            period == INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_PERIOD
            and document_id == INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_DOCUMENT_ID
            and "a significant error was detected" in normalized_page_text
            and "have therefore been significantly revised"
            in normalized_page_text
            and INSEE_CONSUMER_CONFIDENCE_REPLACEMENT_URI.encode("utf-8")
            in snapshot.content
            and not parser.tables
        ):
            raise
        value = None
        unavailable_reason = "withdrawn-after-significant-error"
        replacement_uri = INSEE_CONSUMER_CONFIDENCE_REPLACEMENT_URI
    return InseeConsumerConfidenceReleaseV1(
        source_document_id=document_id,
        reference_period=period,
        title=title,
        subtitle=subtitle,
        published_at=published,
        published_at_local=local.isoformat(),
        source_uri=f"{INSEE_CONSUMER_CONFIDENCE_RELEASE_URI_PREFIX}{document_id}",
        artifact=_artifact(
            snapshot, InseeConsumerConfidenceArtifactRole.RELEASE_HTML
        ),
        value=value,
        value_unavailable_reason=unavailable_reason,
        replacement_source_uri=replacement_uri,
    )


def _parse_catalog(
    snapshot: OfficialRawSnapshotV1,
) -> InseeConsumerConfidenceArtifactV1:
    if (
        snapshot.request.method is not OfficialRequestMethod.POST
        or snapshot.request.body_text != _body(_CATALOG_BODY)
    ):
        raise ValueError("INSEE consumer confidence catalog request differs")
    try:
        payload = json.loads(snapshot.content)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "INSEE consumer confidence catalog JSON is invalid"
        ) from exc
    root = _mapping(payload, "catalog")
    documents = tuple(
        _mapping(item, "catalog document")
        for item in _sequence(root.get("documents"), "documents")
    )
    total = _integer(root.get("numFounds"), "numFounds", 1_000)
    if total != len(documents) or total != 4:
        raise ValueError(
            "INSEE consumer confidence catalog result count differs"
        )
    by_id = {str(item.get("idBank")): item for item in documents}
    if set(by_id) != {
        "000857187",
        "000857199",
        "001587668",
        "011818470",
    }:
        raise ValueError("INSEE consumer confidence catalog inventory differs")
    document = by_id[INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_ID]
    if (
        document.get("idBank") != INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_ID
        or _text(document.get("titre"), "catalog title")
        != "Monthly consumer confidence survey - Summary indicator of households' "
        "confidence (first factor among the opinion survey balances) - SA data"
    ):
        raise ValueError(
            "INSEE consumer confidence catalog series identity differs"
        )
    return _artifact(
        snapshot, InseeConsumerConfidenceArtifactRole.SERIES_CATALOG
    )


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_current_series(
    snapshot: OfficialRawSnapshotV1,
) -> InseeConsumerConfidenceCurrentSeriesV1:
    try:
        root = ET.fromstring(snapshot.content)
    except ET.ParseError as exc:
        raise ValueError(
            "INSEE consumer confidence SDMX XML is invalid"
        ) from exc
    matches = [
        item
        for item in root.iter()
        if _local_name(item.tag) == "Series"
        and item.attrib.get("IDBANK")
        == INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_ID
    ]
    if len(matches) != 1:
        raise ValueError(
            "INSEE consumer confidence SDMX series identity differs"
        )
    series = matches[0]
    if (
        series.attrib.get("FREQ") != "M"
        or series.attrib.get("UNIT_MEASURE") != "SO"
        or series.attrib.get("REF_AREA") != "FM"
        or series.attrib.get("TITLE_EN")
        != "Monthly consumer confidence survey - Summary indicator of households' "
        "confidence (first factor among the opinion survey balances) - SA data"
    ):
        raise ValueError(
            "INSEE consumer confidence SDMX series semantics differ"
        )
    observations = []
    for item in series:
        if _local_name(item.tag) != "Obs":
            continue
        lexical, numeric = _numeric(item.attrib.get("OBS_VALUE"), "OBS_VALUE")
        observations.append(
            InseeConsumerConfidenceCurrentObservationV1(
                reference_period=_month(
                    item.attrib.get("TIME_PERIOD"), "TIME_PERIOD"
                ),
                lexical_value=lexical,
                value=numeric,
                status=str(item.attrib.get("OBS_STATUS", "<missing>")),
            )
        )
    observations.sort(key=lambda item: _month_ordinal(item.reference_period))
    return InseeConsumerConfidenceCurrentSeriesV1(
        series_id=INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_ID,
        title=_text(series.attrib.get("TITLE_EN"), "TITLE_EN"),
        last_update=_iso_date(series.attrib.get("LAST_UPDATE"), "LAST_UPDATE"),
        observations=tuple(observations),
        artifact=_artifact(
            snapshot, InseeConsumerConfidenceArtifactRole.CURRENT_SDMX
        ),
    )


_LIMITATIONS: Final = (
    "The qualified English monthly release-page lineage begins at 2009-06 and has four genuine source-artifact gaps: August 2009, 2010, 2011, and 2012.",
    "INSEE withdrew the erroneous January 2023 publication body after detecting a significant error; the release is retained with no value and its explicit February correction link.",
    "Release pages expose arithmetic-mean, factor-analysis, or normalized-synthetic-index methodology evidence; comparisons require the exact factor-analysis method, metric, unit, adjustment, geography, and reference month.",
    "Release-page values are contemporaneous source-dated occurrences; the BDM series is a latest-revised and backcast cross-check and never replaces a historical vintage.",
    "No official historical market-consensus series was identified, so surprise values are not manufactured.",
)


@dataclass(frozen=True, slots=True)
class InseeConsumerConfidenceArchiveManifestV1:
    """Complete replayable INSEE monthly consumer-confidence manifest."""

    registry_id: str
    source_id: str
    search: InseeConsumerConfidenceSearchV1
    releases: tuple[InseeConsumerConfidenceReleaseV1, ...]
    catalog_artifact: InseeConsumerConfidenceArtifactV1
    current_series: InseeConsumerConfidenceCurrentSeriesV1
    comparisons: tuple[InseeConsumerConfidenceComparisonV1, ...]
    coverage_gaps: tuple[str, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    released_value_count: int
    compatible_comparison_count: int
    limitations: tuple[str, ...] = _LIMITATIONS
    source_key: str = INSEE_CONSUMER_CONFIDENCE_SOURCE_KEY
    program_key: str = INSEE_CONSUMER_CONFIDENCE_PROGRAM_KEY
    parser_id: str = INSEE_CONSUMER_CONFIDENCE_PARSER_ID
    parser_version: str = INSEE_CONSUMER_CONFIDENCE_PARSER_VERSION
    schema_version: str = INSEE_CONSUMER_CONFIDENCE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != INSEE_CONSUMER_CONFIDENCE_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported INSEE consumer confidence manifest schema"
            )
        if (
            self.source_key,
            self.program_key,
            self.parser_id,
            self.parser_version,
        ) != (
            INSEE_CONSUMER_CONFIDENCE_SOURCE_KEY,
            INSEE_CONSUMER_CONFIDENCE_PROGRAM_KEY,
            INSEE_CONSUMER_CONFIDENCE_PARSER_ID,
            INSEE_CONSUMER_CONFIDENCE_PARSER_VERSION,
        ):
            raise ValueError(
                "INSEE consumer confidence manifest program metadata differs"
            )
        for value, prefix, name in (
            (self.registry_id, "official-source-registry", "registry_id"),
            (self.source_id, "official-source", "source_id"),
        ):
            if re.fullmatch(rf"{prefix}:sha256:[0-9a-f]{{64}}", value) is None:
                raise ValueError(
                    f"INSEE consumer confidence manifest {name} is invalid"
                )
        releases = tuple(self.releases)
        if (
            tuple(item.reference_period for item in releases)
            != _release_periods()
        ):
            raise ValueError(
                "INSEE consumer confidence release coverage differs"
            )
        if tuple(item.source_document_id for item in releases) != (
            self.search.selected_document_ids
        ):
            raise ValueError(
                "INSEE consumer confidence release/search order differs"
            )
        if tuple(self.coverage_gaps) != INSEE_CONSUMER_CONFIDENCE_RELEASE_GAPS:
            raise ValueError(
                "INSEE consumer confidence source-gap accounting differs"
            )
        if (
            self.catalog_artifact.role
            is not InseeConsumerConfidenceArtifactRole.SERIES_CATALOG
        ):
            raise ValueError(
                "INSEE consumer confidence catalog artifact role differs"
            )
        expected_comparisons = tuple(
            item.reference_period
            for item in releases
            if item.value is not None
            and item.value.methodology == self.current_series.methodology
        )
        if (
            tuple(item.reference_period for item in self.comparisons)
            != expected_comparisons
        ):
            raise ValueError(
                "INSEE consumer confidence exact-scope comparisons differ"
            )
        artifacts = (
            self.search.artifact,
            *(item.artifact for item in releases),
            self.catalog_artifact,
            self.current_series.artifact,
        )
        if self.raw_artifact_count != len(artifacts):
            raise ValueError(
                "INSEE consumer confidence raw artifact count differs"
            )
        if self.unique_content_sha256_count != len(
            {item.content_sha256 for item in artifacts}
        ):
            raise ValueError(
                "INSEE consumer confidence unique artifact count differs"
            )
        if self.total_content_bytes != sum(
            item.content_length for item in artifacts
        ):
            raise ValueError(
                "INSEE consumer confidence total content bytes differ"
            )
        if (
            not 0
            < self.total_content_bytes
            <= MAX_INSEE_CONSUMER_CONFIDENCE_TOTAL_BYTES
        ):
            raise ValueError(
                "INSEE consumer confidence total content bytes are outside bounds"
            )
        if self.released_value_count != sum(
            item.value is not None for item in releases
        ):
            raise ValueError(
                "INSEE consumer confidence released value count differs"
            )
        if self.compatible_comparison_count != len(self.comparisons):
            raise ValueError(
                "INSEE consumer confidence comparison count differs"
            )
        if tuple(self.limitations) != _LIMITATIONS:
            raise ValueError("INSEE consumer confidence limitations differ")

    @property
    def manifest_id(self) -> str:
        return _stable_id(
            "insee-consumer-confidence-archive-manifest",
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
    ) -> InseeConsumerConfidenceArchiveManifestV1:
        value = cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            search=InseeConsumerConfidenceSearchV1.from_dict(
                _mapping(data.get("search"), "search")
            ),
            releases=tuple(
                InseeConsumerConfidenceReleaseV1.from_dict(
                    _mapping(item, "release")
                )
                for item in _sequence(data.get("releases"), "releases")
            ),
            catalog_artifact=InseeConsumerConfidenceArtifactV1.from_dict(
                _mapping(data.get("catalog_artifact"), "catalog_artifact")
            ),
            current_series=InseeConsumerConfidenceCurrentSeriesV1.from_dict(
                _mapping(data.get("current_series"), "current_series")
            ),
            comparisons=tuple(
                InseeConsumerConfidenceComparisonV1.from_dict(
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
                "INSEE consumer confidence manifest identity differs"
            )
        return value

    @classmethod
    def from_json(
        cls, payload: str
    ) -> InseeConsumerConfidenceArchiveManifestV1:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "INSEE consumer confidence manifest JSON is invalid"
            ) from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_insee_consumer_confidence_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search: InseeConsumerConfidenceSearchV1,
    documents: Sequence[Mapping[str, Any]],
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    catalog_snapshot: OfficialRawSnapshotV1,
    current_series_snapshot: OfficialRawSnapshotV1,
) -> InseeConsumerConfidenceArchiveManifestV1:
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
            "INSEE consumer confidence retained release inventory differs"
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
        InseeConsumerConfidenceComparisonV1(
            reference_period=item.reference_period,
            release_value=item.value.value,
            current_value=current_by_period[item.reference_period],
            delta=round(
                current_by_period[item.reference_period] - item.value.value, 12
            ),
        )
        for item in releases
        if item.value is not None
        and item.value.methodology == current_series.methodology
    )
    artifacts = (
        search.artifact,
        *(item.artifact for item in releases),
        catalog_artifact,
        current_series.artifact,
    )
    return InseeConsumerConfidenceArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        search=search,
        releases=releases,
        catalog_artifact=catalog_artifact,
        current_series=current_series,
        comparisons=comparisons,
        coverage_gaps=INSEE_CONSUMER_CONFIDENCE_RELEASE_GAPS,
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
        released_value_count=sum(item.value is not None for item in releases),
        compatible_comparison_count=len(comparisons),
    )


def replay_insee_consumer_confidence_archive(
    registry: OfficialSourceRegistryV1,
    manifest: InseeConsumerConfidenceArchiveManifestV1,
    search_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    catalog_snapshot: OfficialRawSnapshotV1,
    current_series_snapshot: OfficialRawSnapshotV1,
) -> InseeConsumerConfidenceArchiveManifestV1:
    """Rebuild retained evidence and require byte-independent equality."""
    if registry.registry_id != manifest.registry_id:
        raise ValueError(
            "INSEE consumer confidence replay registry identity differs"
        )
    search, documents = parse_insee_consumer_confidence_search(
        search_snapshot, as_of_date=manifest.search.as_of_date
    )
    rebuilt = build_insee_consumer_confidence_archive_manifest(
        registry,
        search,
        documents,
        release_snapshots,
        catalog_snapshot,
        current_series_snapshot,
    )
    if rebuilt != manifest:
        raise ValueError(
            "INSEE consumer confidence replay differs from retained manifest"
        )
    return rebuilt


def packaged_insee_consumer_confidence_manifest_path() -> Path:
    """Return the packaged INSEE monthly consumer-confidence manifest path."""
    return (
        Path(__file__).with_name("assets")
        / "insee_consumer_confidence_archive_v1.json"
    )


def load_packaged_insee_consumer_confidence_archive_manifest() -> (
    InseeConsumerConfidenceArchiveManifestV1
):
    """Load and fully validate the packaged archive."""
    path = packaged_insee_consumer_confidence_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError(
            "packaged INSEE consumer confidence manifest exceeds size bound"
        )
    return InseeConsumerConfidenceArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )


__all__ = [
    "INSEE_CONSUMER_CONFIDENCE_CATALOG_URI",
    "INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_ID",
    "INSEE_CONSUMER_CONFIDENCE_CURRENT_SERIES_URI",
    "INSEE_CONSUMER_CONFIDENCE_CURRENT_START_PERIOD",
    "INSEE_CONSUMER_CONFIDENCE_LATEST_PACKAGED_PERIOD",
    "INSEE_CONSUMER_CONFIDENCE_PARSER_ID",
    "INSEE_CONSUMER_CONFIDENCE_PARSER_VERSION",
    "INSEE_CONSUMER_CONFIDENCE_PROGRAM_KEY",
    "INSEE_CONSUMER_CONFIDENCE_RELEASE_GAPS",
    "INSEE_CONSUMER_CONFIDENCE_RELEASE_START_PERIOD",
    "INSEE_CONSUMER_CONFIDENCE_REPLACEMENT_URI",
    "INSEE_CONSUMER_CONFIDENCE_SEARCH_URI",
    "INSEE_CONSUMER_CONFIDENCE_SOURCE_KEY",
    "INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_DOCUMENT_ID",
    "INSEE_CONSUMER_CONFIDENCE_WITHDRAWN_PERIOD",
    "InseeConsumerConfidenceArchiveManifestV1",
    "InseeConsumerConfidenceArtifactRole",
    "InseeConsumerConfidenceArtifactV1",
    "InseeConsumerConfidenceComparisonV1",
    "InseeConsumerConfidenceCurrentObservationV1",
    "InseeConsumerConfidenceCurrentSeriesV1",
    "InseeConsumerConfidenceExcludedSearchResultV1",
    "InseeConsumerConfidencePublishedValueV1",
    "InseeConsumerConfidenceReleaseV1",
    "InseeConsumerConfidenceSearchV1",
    "build_insee_consumer_confidence_archive_manifest",
    "build_insee_consumer_confidence_catalog_request",
    "build_insee_consumer_confidence_current_series_request",
    "build_insee_consumer_confidence_release_requests",
    "build_insee_consumer_confidence_search_request",
    "load_packaged_insee_consumer_confidence_archive_manifest",
    "packaged_insee_consumer_confidence_manifest_path",
    "parse_insee_consumer_confidence_search",
    "replay_insee_consumer_confidence_archive",
]
