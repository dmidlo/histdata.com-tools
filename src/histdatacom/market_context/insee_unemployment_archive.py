"""Deterministic qualification of INSEE quarterly ILO-unemployment evidence.

The archive retains one bounded English Solr inventory, every selected
quarterly release page, one exact BDM series-catalog result, and the current
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

INSEE_UNEMPLOYMENT_SOURCE_KEY: Final = "fr.insee.ilo-unemployment"
INSEE_UNEMPLOYMENT_PROGRAM_KEY: Final = "fr.insee.ilo-unemployment"
INSEE_UNEMPLOYMENT_PARSER_ID: Final = "official.insee-ilo-unemployment.v1"
INSEE_UNEMPLOYMENT_PARSER_VERSION: Final = "1"
INSEE_UNEMPLOYMENT_SOURCE_TIMEZONE: Final = "Europe/Paris"
INSEE_UNEMPLOYMENT_SEARCH_URI: Final = (
    "https://www.insee.fr/en/solr/consultation"
)
INSEE_UNEMPLOYMENT_CATALOG_URI: Final = (
    "https://www.insee.fr/en/statistiques/series/ajax/consultation"
)
INSEE_UNEMPLOYMENT_RELEASE_URI_PREFIX: Final = (
    "https://www.insee.fr/en/statistiques/"
)
INSEE_UNEMPLOYMENT_CURRENT_SERIES_ID: Final = "011818543"
INSEE_UNEMPLOYMENT_CURRENT_SERIES_URI: Final = (
    "https://bdm.insee.fr/series/sdmx/data/SERIES_BDM/011818543"
    "?startPeriod=2000-Q1&endPeriod=2026-Q2"
)
INSEE_UNEMPLOYMENT_RELEASE_START_PERIOD: Final = "2009-Q1"
INSEE_UNEMPLOYMENT_CURRENT_START_PERIOD: Final = "2003-Q1"
INSEE_UNEMPLOYMENT_LATEST_PACKAGED_PERIOD: Final = "2026-Q2"
INSEE_UNEMPLOYMENT_RELEASE_GAPS: Final = ("2013-Q1",)

INSEE_UNEMPLOYMENT_ARTIFACT_SCHEMA_VERSION: Final = (
    "histdatacom.insee-unemployment-artifact.v1"
)
INSEE_UNEMPLOYMENT_EXCLUSION_SCHEMA_VERSION: Final = (
    "histdatacom.insee-unemployment-exclusion.v1"
)
INSEE_UNEMPLOYMENT_SEARCH_SCHEMA_VERSION: Final = (
    "histdatacom.insee-unemployment-search.v1"
)
INSEE_UNEMPLOYMENT_VALUE_SCHEMA_VERSION: Final = (
    "histdatacom.insee-unemployment-value.v1"
)
INSEE_UNEMPLOYMENT_RELEASE_SCHEMA_VERSION: Final = (
    "histdatacom.insee-unemployment-release.v1"
)
INSEE_UNEMPLOYMENT_OBSERVATION_SCHEMA_VERSION: Final = (
    "histdatacom.insee-unemployment-current-observation.v1"
)
INSEE_UNEMPLOYMENT_SERIES_SCHEMA_VERSION: Final = (
    "histdatacom.insee-unemployment-current-series.v1"
)
INSEE_UNEMPLOYMENT_COMPARISON_SCHEMA_VERSION: Final = (
    "histdatacom.insee-unemployment-comparison.v1"
)
INSEE_UNEMPLOYMENT_MANIFEST_SCHEMA_VERSION: Final = (
    "histdatacom.insee-unemployment-archive-manifest.v1"
)

MAX_INSEE_UNEMPLOYMENT_RESULTS: Final = 1_000
MAX_INSEE_UNEMPLOYMENT_ARTIFACT_BYTES: Final = 64 * 1024 * 1024
MAX_INSEE_UNEMPLOYMENT_TOTAL_BYTES: Final = 512 * 1024 * 1024
_SPACE_RE = re.compile(r"\s+")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMBER_RE = re.compile(r"^[+\-]?\d+(?:[.,]\d+)?$")
_QUARTER_RE = re.compile(r"^(?P<year>\d{4})-Q(?P<quarter>[1-4])$")
_SUBTITLE_RE = re.compile(
    r"^ILO Unemployment and Labour Market-related indicators "
    r"\(Labour Force Survey results\) - "
    r"(?P<quarter>first|second|third|fourth|1st|2nd|3rd|4th|4st) "
    r"quarter (?P<year>\d{4})$",
    re.IGNORECASE,
)
_QUARTER_NUMBER: Final = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "1st": 1,
    "2nd": 2,
    "3rd": 3,
    "4th": 4,
    "4st": 4,
}
_SEARCH_BODY: Final[Mapping[str, JSONValue]] = {
    "facetsQuery": [],
    "filters": [
        {"field": "diffusion", "values": [True]},
        {"field": "rubrique", "values": ["statistiques"]},
    ],
    "q": "unemployment rate",
    "rows": 1000,
    "sortFields": [{"field": "dateDiffusion", "order": "desc"}],
    "start": 0,
}
_CATALOG_BODY: Final[Mapping[str, JSONValue]] = {
    "facetsField": [],
    "filters": [{"field": "bdm_idFamille", "values": ["103167923"]}],
    "q": '"ILO unemployment rate - Total - France - SA data"',
    "rows": 1000,
    "sortFields": [],
    "start": 0,
}
_GEOGRAPHIES: Final = {
    "france",
    "france-excluding-mayotte",
    "metropolitan-france-plus-overseas-departments",
    "metropolitan-france",
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


def _quarter(value: object, name: str) -> str:
    result = _text(value, name)
    if _QUARTER_RE.fullmatch(result) is None:
        raise ValueError(f"{name} must be YYYY-Qn")
    return result


def _quarter_ordinal(value: str) -> int:
    match = cast(
        re.Match[str], _QUARTER_RE.fullmatch(_quarter(value, "quarter"))
    )
    return int(match.group("year")) * 4 + int(match.group("quarter")) - 1


def _quarter_range(start: str, end: str) -> tuple[str, ...]:
    first = _quarter_ordinal(start)
    last = _quarter_ordinal(end)
    if first > last:
        raise ValueError("quarter range is reversed")
    return tuple(
        f"{ordinal // 4}-Q{ordinal % 4 + 1}"
        for ordinal in range(first, last + 1)
    )


def _release_periods() -> tuple[str, ...]:
    gaps = set(INSEE_UNEMPLOYMENT_RELEASE_GAPS)
    return tuple(
        period
        for period in _quarter_range(
            INSEE_UNEMPLOYMENT_RELEASE_START_PERIOD,
            INSEE_UNEMPLOYMENT_LATEST_PACKAGED_PERIOD,
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
    if not math.isfinite(numeric) or not 0.0 <= numeric <= 100.0:
        raise ValueError(f"{name} is outside its bound")
    return lexical, numeric


def _body(payload: Mapping[str, JSONValue]) -> str:
    return str(canonical_contract_json(dict(payload)))


def _source(registry: OfficialSourceRegistryV1) -> OfficialSourceEntryV1:
    source = registry.source(INSEE_UNEMPLOYMENT_SOURCE_KEY)
    if (
        source.parser_id != INSEE_UNEMPLOYMENT_PARSER_ID
        or source.parser_version != INSEE_UNEMPLOYMENT_PARSER_VERSION
    ):
        raise ValueError("INSEE unemployment registry parser binding differs")
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
            "INSEE unemployment request host differs from registry"
        )
    if source_format not in source.formats:
        raise ValueError(
            "INSEE unemployment request format differs from registry"
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


def build_insee_unemployment_search_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact bounded Solr request used for release discovery."""
    return _request(
        registry,
        INSEE_UNEMPLOYMENT_SEARCH_URI,
        OfficialSourceFormat.JSON,
        method=OfficialRequestMethod.POST,
        body=_SEARCH_BODY,
    )


def build_insee_unemployment_release_requests(
    registry: OfficialSourceRegistryV1,
    documents: Sequence[Mapping[str, Any]],
) -> tuple[OfficialSourceRequestV1, ...]:
    """Build one exact HTML request per selected release document."""
    return tuple(
        _request(
            registry,
            f"{INSEE_UNEMPLOYMENT_RELEASE_URI_PREFIX}"
            f"{_integer(item.get('id'), 'document id', 1_000_000_000)}",
            OfficialSourceFormat.HTML,
        )
        for item in documents
    )


def build_insee_unemployment_catalog_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the exact series-catalog request proving the selected BDM ID."""
    return _request(
        registry,
        INSEE_UNEMPLOYMENT_CATALOG_URI,
        OfficialSourceFormat.JSON,
        method=OfficialRequestMethod.POST,
        body=_CATALOG_BODY,
    )


def build_insee_unemployment_current_series_request(
    registry: OfficialSourceRegistryV1,
) -> OfficialSourceRequestV1:
    """Build the bounded latest-state BDM unemployment-series request."""
    return _request(
        registry,
        INSEE_UNEMPLOYMENT_CURRENT_SERIES_URI,
        OfficialSourceFormat.SDMX_21,
    )


class InseeUnemploymentArtifactRole(str, Enum):
    """Semantic role of one retained official response."""

    RELEASE_SEARCH = "release-search"
    RELEASE_HTML = "release-html"
    SERIES_CATALOG = "series-catalog"
    CURRENT_SDMX = "current-sdmx"


@dataclass(frozen=True, slots=True)
class InseeUnemploymentArtifactV1:
    """Hash-bound receipt for one exact official response."""

    role: InseeUnemploymentArtifactRole
    source_uri: str
    source_format: OfficialSourceFormat
    content_sha256: str
    content_length: int
    request_id: str
    schema_version: str = INSEE_UNEMPLOYMENT_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_UNEMPLOYMENT_ARTIFACT_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE unemployment artifact schema")
        if not isinstance(self.role, InseeUnemploymentArtifactRole):
            raise TypeError("INSEE unemployment artifact role is invalid")
        if not isinstance(self.source_format, OfficialSourceFormat):
            raise TypeError("INSEE unemployment artifact format is invalid")
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        if _DIGEST_RE.fullmatch(self.content_sha256) is None:
            raise ValueError("INSEE unemployment artifact digest is invalid")
        if not 0 < self.content_length <= MAX_INSEE_UNEMPLOYMENT_ARTIFACT_BYTES:
            raise ValueError("INSEE unemployment artifact length is invalid")
        if (
            re.fullmatch(
                r"official-request:sha256:[0-9a-f]{64}", self.request_id
            )
            is None
        ):
            raise ValueError("INSEE unemployment request identity is invalid")

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
    def from_dict(cls, data: Mapping[str, Any]) -> InseeUnemploymentArtifactV1:
        return cls(
            role=InseeUnemploymentArtifactRole(str(data.get("role", ""))),
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
    snapshot: OfficialRawSnapshotV1, role: InseeUnemploymentArtifactRole
) -> InseeUnemploymentArtifactV1:
    if (
        snapshot.request.source_key != INSEE_UNEMPLOYMENT_SOURCE_KEY
        or snapshot.request.parser_id != INSEE_UNEMPLOYMENT_PARSER_ID
        or snapshot.request.parser_version != INSEE_UNEMPLOYMENT_PARSER_VERSION
    ):
        raise ValueError("INSEE unemployment snapshot names another source")
    return InseeUnemploymentArtifactV1(
        role=role,
        source_uri=snapshot.request.uri,
        source_format=snapshot.request.source_format,
        content_sha256=snapshot.content_sha256,
        content_length=len(snapshot.content),
        request_id=snapshot.request.request_id,
    )


@dataclass(frozen=True, slots=True)
class InseeUnemploymentExcludedSearchResultV1:
    """One explicitly classified but unselected Solr result."""

    source_document_id: int
    title: str
    subtitle: str | None
    reason: str
    schema_version: str = INSEE_UNEMPLOYMENT_EXCLUSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_UNEMPLOYMENT_EXCLUSION_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE unemployment exclusion schema")
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError(
                "INSEE unemployment excluded document ID is invalid"
            )
        object.__setattr__(self, "title", _text(self.title, "title"))
        if self.subtitle is not None:
            object.__setattr__(
                self, "subtitle", _text(self.subtitle, "subtitle")
            )
        if self.reason not in {
            "missing-subtitle",
            "outside-packaged-coverage",
            "other-unemployment-publication",
            "search-result-outside-release-lineage",
        }:
            raise ValueError("INSEE unemployment exclusion reason is invalid")

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
    ) -> InseeUnemploymentExcludedSearchResultV1:
        raw_subtitle = data.get("subtitle")
        return cls(
            source_document_id=cast(int, data.get("source_document_id")),
            title=str(data.get("title", "")),
            subtitle=None if raw_subtitle is None else str(raw_subtitle),
            reason=str(data.get("reason", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeUnemploymentSearchV1:
    """Complete accounting of the bounded release-search response."""

    as_of_date: str
    artifact: InseeUnemploymentArtifactV1
    total_result_count: int
    selected_document_ids: tuple[int, ...]
    exclusions: tuple[InseeUnemploymentExcludedSearchResultV1, ...]
    schema_version: str = INSEE_UNEMPLOYMENT_SEARCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_UNEMPLOYMENT_SEARCH_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE unemployment search schema")
        object.__setattr__(
            self, "as_of_date", _iso_date(self.as_of_date, "as_of_date")
        )
        if (
            self.artifact.role
            is not InseeUnemploymentArtifactRole.RELEASE_SEARCH
        ):
            raise ValueError("INSEE unemployment search artifact role differs")
        if not 0 < self.total_result_count <= MAX_INSEE_UNEMPLOYMENT_RESULTS:
            raise ValueError(
                "INSEE unemployment search result count is invalid"
            )
        ids = tuple(self.selected_document_ids)
        if len(ids) != len(set(ids)):
            raise ValueError("INSEE unemployment selected IDs are not unique")
        excluded_ids = {item.source_document_id for item in self.exclusions}
        if len(ids) + len(excluded_ids) != self.total_result_count:
            raise ValueError(
                "INSEE unemployment selected/excluded accounting differs"
            )
        if set(ids) & excluded_ids:
            raise ValueError(
                "INSEE unemployment selected and excluded IDs overlap"
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
    def from_dict(cls, data: Mapping[str, Any]) -> InseeUnemploymentSearchV1:
        return cls(
            as_of_date=str(data.get("as_of_date", "")),
            artifact=InseeUnemploymentArtifactV1.from_dict(
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
                InseeUnemploymentExcludedSearchResultV1.from_dict(
                    _mapping(item, "exclusion")
                )
                for item in _sequence(data.get("exclusions"), "exclusions")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeUnemploymentPublishedValueV1:
    """One contemporaneous total ILO-unemployment-rate value."""

    lexical_value: str
    value: float
    locator: str
    geography_scope: str
    metric: str = "ilo-unemployment-rate"
    unit: str = "percent-of-labour-force"
    adjustment: str = "seasonally-adjusted-quarterly-average"
    sex: str = "all"
    age_group: str = "15-and-over"
    schema_version: str = INSEE_UNEMPLOYMENT_VALUE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_UNEMPLOYMENT_VALUE_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE unemployment value schema")
        lexical, parsed = _numeric(self.lexical_value, "lexical_value")
        object.__setattr__(self, "lexical_value", lexical)
        if not isinstance(self.value, (float, int)) or isinstance(
            self.value, bool
        ):
            raise TypeError("INSEE unemployment value must be numeric")
        if abs(float(self.value) - parsed) > 1e-12:
            raise ValueError(
                "INSEE unemployment lexical and numeric values differ"
            )
        object.__setattr__(self, "value", float(self.value))
        object.__setattr__(self, "locator", _text(self.locator, "locator"))
        if self.geography_scope not in _GEOGRAPHIES:
            raise ValueError("INSEE unemployment geography scope differs")
        if (
            self.metric,
            self.unit,
            self.adjustment,
            self.sex,
            self.age_group,
        ) != (
            "ilo-unemployment-rate",
            "percent-of-labour-force",
            "seasonally-adjusted-quarterly-average",
            "all",
            "15-and-over",
        ):
            raise ValueError("INSEE unemployment value semantics differ")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "metric": self.metric,
            "unit": self.unit,
            "adjustment": self.adjustment,
            "geography_scope": self.geography_scope,
            "sex": self.sex,
            "age_group": self.age_group,
            "lexical_value": self.lexical_value,
            "value": self.value,
            "locator": self.locator,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeUnemploymentPublishedValueV1:
        return cls(
            lexical_value=str(data.get("lexical_value", "")),
            value=float(cast(float, data.get("value"))),
            locator=str(data.get("locator", "")),
            geography_scope=str(data.get("geography_scope", "")),
            metric=str(data.get("metric", "")),
            unit=str(data.get("unit", "")),
            adjustment=str(data.get("adjustment", "")),
            sex=str(data.get("sex", "")),
            age_group=str(data.get("age_group", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def _subtitle_period(subtitle: str) -> str:
    match = _SUBTITLE_RE.fullmatch(_text(subtitle, "subtitle"))
    if match is None:
        raise ValueError(
            "INSEE unemployment subtitle is outside the selected lineage"
        )
    quarter = _QUARTER_NUMBER[match.group("quarter").casefold()]
    return f"{match.group('year')}-Q{quarter}"


@dataclass(frozen=True, slots=True)
class InseeUnemploymentReleaseV1:
    """One source-dated INSEE quarterly unemployment publication."""

    source_document_id: int
    reference_period: str
    title: str
    subtitle: str
    published_at: str
    published_at_local: str
    source_uri: str
    artifact: InseeUnemploymentArtifactV1
    value: InseeUnemploymentPublishedValueV1
    schema_version: str = INSEE_UNEMPLOYMENT_RELEASE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_UNEMPLOYMENT_RELEASE_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE unemployment release schema")
        if not 0 < self.source_document_id < 1_000_000_000:
            raise ValueError("INSEE unemployment document ID is invalid")
        object.__setattr__(
            self,
            "reference_period",
            _quarter(self.reference_period, "reference_period"),
        )
        if _subtitle_period(self.subtitle) != self.reference_period:
            raise ValueError("INSEE unemployment subtitle period differs")
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
                "INSEE unemployment local publication timestamp differs"
            )
        object.__setattr__(self, "source_uri", _official_uri(self.source_uri))
        expected_uri = (
            f"{INSEE_UNEMPLOYMENT_RELEASE_URI_PREFIX}{self.source_document_id}"
        )
        if (
            self.source_uri != expected_uri
            or self.artifact.source_uri != expected_uri
        ):
            raise ValueError("INSEE unemployment release URI differs")
        if self.artifact.role is not InseeUnemploymentArtifactRole.RELEASE_HTML:
            raise ValueError("INSEE unemployment release artifact role differs")

    @property
    def release_id(self) -> str:
        return _stable_id("insee-unemployment-release", self.identity_payload())

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
    def from_dict(cls, data: Mapping[str, Any]) -> InseeUnemploymentReleaseV1:
        value = cls(
            source_document_id=cast(int, data.get("source_document_id")),
            reference_period=str(data.get("reference_period", "")),
            title=str(data.get("title", "")),
            subtitle=str(data.get("subtitle", "")),
            published_at=str(data.get("published_at", "")),
            published_at_local=str(data.get("published_at_local", "")),
            source_uri=str(data.get("source_uri", "")),
            artifact=InseeUnemploymentArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            value=InseeUnemploymentPublishedValueV1.from_dict(
                _mapping(data.get("value"), "value")
            ),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("release_id") != value.release_id:
            raise ValueError("INSEE unemployment release identity differs")
        return value


@dataclass(frozen=True, slots=True)
class InseeUnemploymentCurrentObservationV1:
    """One latest-revised observation from the BDM unemployment series."""

    reference_period: str
    lexical_value: str
    value: float
    status: str
    schema_version: str = INSEE_UNEMPLOYMENT_OBSERVATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_UNEMPLOYMENT_OBSERVATION_SCHEMA_VERSION:
            raise ValueError(
                "unsupported INSEE unemployment observation schema"
            )
        object.__setattr__(
            self,
            "reference_period",
            _quarter(self.reference_period, "reference_period"),
        )
        lexical, numeric = _numeric(self.lexical_value, "lexical_value")
        object.__setattr__(self, "lexical_value", lexical)
        if abs(float(self.value) - numeric) > 1e-12:
            raise ValueError("INSEE unemployment observation lexical differs")
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
    ) -> InseeUnemploymentCurrentObservationV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            lexical_value=str(data.get("lexical_value", "")),
            value=float(cast(float, data.get("value"))),
            status=str(data.get("status", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeUnemploymentCurrentSeriesV1:
    """Latest revised BDM series, explicitly separate from release vintages."""

    series_id: str
    title: str
    last_update: str
    observations: tuple[InseeUnemploymentCurrentObservationV1, ...]
    artifact: InseeUnemploymentArtifactV1
    metric: str = "ilo-unemployment-rate"
    unit: str = "percent-of-labour-force"
    adjustment: str = "seasonally-adjusted-quarterly-average"
    geography_scope: str = "france"
    sex: str = "all"
    age_group: str = "15-and-over"
    semantics: str = "latest-revised-cross-check"
    schema_version: str = INSEE_UNEMPLOYMENT_SERIES_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_UNEMPLOYMENT_SERIES_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE unemployment series schema")
        if self.series_id != INSEE_UNEMPLOYMENT_CURRENT_SERIES_ID:
            raise ValueError(
                "INSEE unemployment current series identity differs"
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
            self.sex,
            self.age_group,
            self.semantics,
        ) != (
            "ilo-unemployment-rate",
            "percent-of-labour-force",
            "seasonally-adjusted-quarterly-average",
            "france",
            "all",
            "15-and-over",
            "latest-revised-cross-check",
        ):
            raise ValueError(
                "INSEE unemployment current series semantics differ"
            )
        values = tuple(self.observations)
        expected = _quarter_range(
            INSEE_UNEMPLOYMENT_CURRENT_START_PERIOD,
            INSEE_UNEMPLOYMENT_LATEST_PACKAGED_PERIOD,
        )
        if tuple(item.reference_period for item in values) != expected:
            raise ValueError(
                "INSEE unemployment current-series coverage differs"
            )
        if self.artifact.role is not InseeUnemploymentArtifactRole.CURRENT_SDMX:
            raise ValueError(
                "INSEE unemployment current-series artifact role differs"
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
            "sex": self.sex,
            "age_group": self.age_group,
            "semantics": self.semantics,
            "observations": [item.to_dict() for item in self.observations],
            "artifact": self.artifact.to_dict(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeUnemploymentCurrentSeriesV1:
        return cls(
            series_id=str(data.get("series_id", "")),
            title=str(data.get("title", "")),
            last_update=str(data.get("last_update", "")),
            observations=tuple(
                InseeUnemploymentCurrentObservationV1.from_dict(
                    _mapping(item, "observation")
                )
                for item in _sequence(data.get("observations"), "observations")
            ),
            artifact=InseeUnemploymentArtifactV1.from_dict(
                _mapping(data.get("artifact"), "artifact")
            ),
            metric=str(data.get("metric", "")),
            unit=str(data.get("unit", "")),
            adjustment=str(data.get("adjustment", "")),
            geography_scope=str(data.get("geography_scope", "")),
            sex=str(data.get("sex", "")),
            age_group=str(data.get("age_group", "")),
            semantics=str(data.get("semantics", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class InseeUnemploymentComparisonV1:
    """Exact-scope release versus latest-revised BDM comparison."""

    reference_period: str
    release_value: float
    current_value: float
    delta: float
    geography_scope: str = "france"
    schema_version: str = INSEE_UNEMPLOYMENT_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_UNEMPLOYMENT_COMPARISON_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE unemployment comparison schema")
        object.__setattr__(
            self,
            "reference_period",
            _quarter(self.reference_period, "reference_period"),
        )
        if self.geography_scope != "france":
            raise ValueError("INSEE unemployment comparison scope differs")
        expected = round(
            float(self.current_value) - float(self.release_value), 12
        )
        if abs(float(self.delta) - expected) > 1e-12:
            raise ValueError("INSEE unemployment comparison delta differs")
        object.__setattr__(self, "release_value", float(self.release_value))
        object.__setattr__(self, "current_value", float(self.current_value))
        object.__setattr__(self, "delta", float(self.delta))

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_period": self.reference_period,
            "geography_scope": self.geography_scope,
            "release_value": self.release_value,
            "current_value": self.current_value,
            "delta": self.delta,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> InseeUnemploymentComparisonV1:
        return cls(
            reference_period=str(data.get("reference_period", "")),
            release_value=float(cast(float, data.get("release_value"))),
            current_value=float(cast(float, data.get("current_value"))),
            delta=float(cast(float, data.get("delta"))),
            geography_scope=str(data.get("geography_scope", "")),
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
        raise ValueError("INSEE unemployment release is not UTF-8") from exc
    parser = _ReleaseHtmlParser()
    parser.feed(source)
    parser.close()
    return parser


def _header_period(value: str) -> str | None:
    normalized = _SPACE_RE.sub(" ", value.replace("-", " ")).strip()
    match = re.fullmatch(
        r"(?:(\d{4})\s*Q([1-4])|Q([1-4])\s*(\d{4}))(?:\s*\([^)]*\))?\*?",
        normalized,
        re.IGNORECASE,
    )
    if match is None:
        return None
    year = match.group(1) or match.group(4)
    quarter = match.group(2) or match.group(3)
    return f"{year}-Q{quarter}"


def _period_geography(period: str) -> str:
    if period == "2026-Q2":
        return "france"
    if period >= "2014-Q1":
        return "france-excluding-mayotte"
    return "metropolitan-france-plus-overseas-departments"


def _chart_value(
    parser: _ReleaseHtmlParser, period: str
) -> InseeUnemploymentPublishedValueV1 | None:
    for table_index, table in enumerate(parser.tables):
        if table.caption.casefold().replace(
            "ilo unemployment", "ilo-unemployment"
        ) != ("ilo-unemployment rate"):
            continue
        if not table.rows:
            continue
        header = table.rows[0]
        for row_index, row in enumerate(table.rows[1:], 1):
            if not row or _header_period(row[0]) != period or len(row) < 2:
                continue
            lexical, numeric = _numeric(row[1], "unemployment chart value")
            header_scope = header[1].casefold() if len(header) > 1 else ""
            if "excl" in header_scope and "mayotte" in header_scope:
                geography = "france-excluding-mayotte"
            elif "metropolitan" in header_scope:
                geography = "metropolitan-france"
            else:
                geography = _period_geography(period)
            return InseeUnemploymentPublishedValueV1(
                lexical_value=lexical,
                value=numeric,
                geography_scope=geography,
                locator=(
                    f"table:{table_index}:{table.table_id}:row:{row_index}:column:1"
                ),
            )
    return None


def _description_value(
    parser: _ReleaseHtmlParser, period: str
) -> InseeUnemploymentPublishedValueV1 | None:
    description = parser.meta_description
    if not description:
        return None
    lowered = description.casefold()
    marker = lowered.find("unemployment rate")
    if marker < 0:
        return None
    window = description[marker : marker + 600]
    matches = tuple(re.finditer(r"(?P<value>\d{1,2}(?:[.,]\d+)?)\s*%", window))
    if not matches:
        return None
    selected = matches[0]
    between = window[: selected.start()].casefold()
    if (
        " from " in between
        and " to " in window[: matches[-1].start()].casefold()
    ):
        selected = matches[-1]
    lexical, numeric = _numeric(selected.group("value"), "description rate")
    if "metropolitan france and overseas departments" in lowered:
        geography = "metropolitan-france-plus-overseas-departments"
    elif (
        "france (excluding mayotte)" in lowered
        or "france excluding mayotte" in lowered
    ):
        geography = "france-excluding-mayotte"
    elif "metropolitan france" in lowered:
        geography = "metropolitan-france"
    else:
        geography = _period_geography(period)
    return InseeUnemploymentPublishedValueV1(
        lexical_value=lexical,
        value=numeric,
        geography_scope=geography,
        locator="meta:description",
    )


def _detail_table_value(
    parser: _ReleaseHtmlParser, period: str
) -> InseeUnemploymentPublishedValueV1 | None:
    labels = {"total", "unemployed persons"}
    for table_index, table in enumerate(parser.tables):
        caption = table.caption.casefold().replace(
            "ilo unemployment", "ilo-unemployment"
        )
        if "ilo-unemployment rate" not in caption:
            continue
        for header_index, header in enumerate(table.rows):
            periods = tuple(_header_period(cell) for cell in header)
            if period not in periods:
                continue
            column = periods.index(period)
            for row_index, row in enumerate(
                table.rows[header_index + 1 :], header_index + 1
            ):
                if (
                    not row
                    or row[0].casefold() not in labels
                    or column >= len(row)
                ):
                    continue
                lexical, numeric = _numeric(
                    row[column], "unemployment detail-table value"
                )
                geography = (
                    "metropolitan-france"
                    if "metropolitan france" in caption
                    else _period_geography(period)
                )
                return InseeUnemploymentPublishedValueV1(
                    lexical_value=lexical,
                    value=numeric,
                    geography_scope=geography,
                    locator=(
                        f"table:{table_index}:{table.table_id}:"
                        f"row:{row_index}:column:{column}"
                    ),
                )
    return None


def _release_value(
    parser: _ReleaseHtmlParser, period: str
) -> InseeUnemploymentPublishedValueV1:
    value = (
        _chart_value(parser, period)
        or _description_value(parser, period)
        or _detail_table_value(parser, period)
    )
    if value is None:
        raise ValueError(
            f"INSEE unemployment release omits a total rate for {period}"
        )
    return value


def parse_insee_unemployment_search(
    snapshot: OfficialRawSnapshotV1,
    *,
    as_of_date: str,
) -> tuple[InseeUnemploymentSearchV1, tuple[Mapping[str, Any], ...]]:
    """Classify every Solr result and retain the quarterly release lineage."""
    boundary = _iso_date(as_of_date, "as_of_date")
    if (
        snapshot.request.method is not OfficialRequestMethod.POST
        or snapshot.request.body_text != _body(_SEARCH_BODY)
    ):
        raise ValueError("INSEE unemployment search request differs")
    try:
        payload = json.loads(snapshot.content)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("INSEE unemployment search JSON is invalid") from exc
    root = _mapping(payload, "search")
    documents = tuple(
        _mapping(item, "search document")
        for item in _sequence(root.get("documents"), "documents")
    )
    total = _integer(
        root.get("numFounds"), "numFounds", MAX_INSEE_UNEMPLOYMENT_RESULTS
    )
    if total != len(documents):
        raise ValueError("INSEE unemployment Solr response is truncated")
    selected: list[Mapping[str, Any]] = []
    exclusions: list[InseeUnemploymentExcludedSearchResultV1] = []
    seen: set[int] = set()
    for document in documents:
        document_id = _integer(document.get("id"), "document id", 1_000_000_000)
        if document_id in seen:
            raise ValueError(
                "INSEE unemployment Solr response repeats a document"
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
        elif "unemployment" in subtitle.casefold():
            reason = "other-unemployment-publication"
        else:
            reason = "search-result-outside-release-lineage"
        exclusions.append(
            InseeUnemploymentExcludedSearchResultV1(
                document_id, title, subtitle, reason
            )
        )
    selected.sort(key=lambda item: _subtitle_period(str(item["sousTitre"])))
    search = InseeUnemploymentSearchV1(
        as_of_date=boundary,
        artifact=_artifact(
            snapshot, InseeUnemploymentArtifactRole.RELEASE_SEARCH
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
) -> InseeUnemploymentReleaseV1:
    document_id = _integer(document.get("id"), "document id", 1_000_000_000)
    title = _text(document.get("titre"), "title")
    subtitle = _text(document.get("sousTitre"), "subtitle")
    period = _subtitle_period(subtitle)
    published = _timestamp(document.get("dateDiffusion"), "dateDiffusion")
    local = datetime.fromisoformat(published.replace("Z", "+00:00")).astimezone(
        ZoneInfo(INSEE_UNEMPLOYMENT_SOURCE_TIMEZONE)
    )
    parser = _parse_html(snapshot.content)
    page_text = _SPACE_RE.sub(" ", " ".join(parser.text)).strip()
    if title not in page_text and subtitle not in page_text:
        raise ValueError(
            f"INSEE unemployment page metadata differs: {document_id}"
        )
    return InseeUnemploymentReleaseV1(
        source_document_id=document_id,
        reference_period=period,
        title=title,
        subtitle=subtitle,
        published_at=published,
        published_at_local=local.isoformat(),
        source_uri=f"{INSEE_UNEMPLOYMENT_RELEASE_URI_PREFIX}{document_id}",
        artifact=_artifact(
            snapshot, InseeUnemploymentArtifactRole.RELEASE_HTML
        ),
        value=_release_value(parser, period),
    )


def _parse_catalog(
    snapshot: OfficialRawSnapshotV1,
) -> InseeUnemploymentArtifactV1:
    if (
        snapshot.request.method is not OfficialRequestMethod.POST
        or snapshot.request.body_text != _body(_CATALOG_BODY)
    ):
        raise ValueError("INSEE unemployment catalog request differs")
    try:
        payload = json.loads(snapshot.content)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("INSEE unemployment catalog JSON is invalid") from exc
    root = _mapping(payload, "catalog")
    documents = tuple(
        _mapping(item, "catalog document")
        for item in _sequence(root.get("documents"), "documents")
    )
    total = _integer(root.get("numFounds"), "numFounds", 1_000)
    if total != len(documents) or total != 1:
        raise ValueError("INSEE unemployment catalog result count differs")
    document = documents[0]
    if (
        document.get("idBank") != INSEE_UNEMPLOYMENT_CURRENT_SERIES_ID
        or _text(document.get("titre"), "catalog title")
        != "ILO unemployment rate - Total - France - SA data"
    ):
        raise ValueError("INSEE unemployment catalog series identity differs")
    return _artifact(snapshot, InseeUnemploymentArtifactRole.SERIES_CATALOG)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _parse_current_series(
    snapshot: OfficialRawSnapshotV1,
) -> InseeUnemploymentCurrentSeriesV1:
    try:
        root = ET.fromstring(snapshot.content)
    except ET.ParseError as exc:
        raise ValueError("INSEE unemployment SDMX XML is invalid") from exc
    matches = [
        item
        for item in root.iter()
        if _local_name(item.tag) == "Series"
        and item.attrib.get("IDBANK") == INSEE_UNEMPLOYMENT_CURRENT_SERIES_ID
    ]
    if len(matches) != 1:
        raise ValueError("INSEE unemployment SDMX series identity differs")
    series = matches[0]
    if (
        series.attrib.get("FREQ") != "T"
        or series.attrib.get("UNIT_MEASURE") != "POURCENT"
        or series.attrib.get("REF_AREA") != "FE"
        or series.attrib.get("TITLE_EN")
        != "ILO unemployment rate - Total - France - SA data"
    ):
        raise ValueError("INSEE unemployment SDMX series semantics differ")
    observations = []
    for item in series:
        if _local_name(item.tag) != "Obs":
            continue
        lexical, numeric = _numeric(item.attrib.get("OBS_VALUE"), "OBS_VALUE")
        observations.append(
            InseeUnemploymentCurrentObservationV1(
                reference_period=_quarter(
                    item.attrib.get("TIME_PERIOD"), "TIME_PERIOD"
                ),
                lexical_value=lexical,
                value=numeric,
                status=str(item.attrib.get("OBS_STATUS", "<missing>")),
            )
        )
    observations.sort(key=lambda item: _quarter_ordinal(item.reference_period))
    return InseeUnemploymentCurrentSeriesV1(
        series_id=INSEE_UNEMPLOYMENT_CURRENT_SERIES_ID,
        title=_text(series.attrib.get("TITLE_EN"), "TITLE_EN"),
        last_update=_iso_date(series.attrib.get("LAST_UPDATE"), "LAST_UPDATE"),
        observations=tuple(observations),
        artifact=_artifact(
            snapshot, InseeUnemploymentArtifactRole.CURRENT_SDMX
        ),
    )


_LIMITATIONS: Final = (
    "The qualified English quarterly release-page lineage begins at 2009-Q1 and has one genuine source-artifact gap at 2013-Q1; the 2013-Q2 page records that only a partial estimate was released for the missing quarter.",
    "Geographic coverage and Labour Force Survey methods change across the retained history; comparisons require identical geography, metric, population, sex, age, adjustment, and reference period.",
    "Release-page values are contemporaneous source-dated occurrences; the BDM series is a latest-revised and backcast cross-check and never replaces a historical vintage.",
    "No official historical market-consensus series was identified, so surprise values are not manufactured.",
)


@dataclass(frozen=True, slots=True)
class InseeUnemploymentArchiveManifestV1:
    """Complete replayable INSEE quarterly unemployment evidence manifest."""

    registry_id: str
    source_id: str
    search: InseeUnemploymentSearchV1
    releases: tuple[InseeUnemploymentReleaseV1, ...]
    catalog_artifact: InseeUnemploymentArtifactV1
    current_series: InseeUnemploymentCurrentSeriesV1
    comparisons: tuple[InseeUnemploymentComparisonV1, ...]
    coverage_gaps: tuple[str, ...]
    raw_artifact_count: int
    unique_content_sha256_count: int
    total_content_bytes: int
    released_value_count: int
    compatible_comparison_count: int
    limitations: tuple[str, ...] = _LIMITATIONS
    source_key: str = INSEE_UNEMPLOYMENT_SOURCE_KEY
    program_key: str = INSEE_UNEMPLOYMENT_PROGRAM_KEY
    parser_id: str = INSEE_UNEMPLOYMENT_PARSER_ID
    parser_version: str = INSEE_UNEMPLOYMENT_PARSER_VERSION
    schema_version: str = INSEE_UNEMPLOYMENT_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != INSEE_UNEMPLOYMENT_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported INSEE unemployment manifest schema")
        if (
            self.source_key,
            self.program_key,
            self.parser_id,
            self.parser_version,
        ) != (
            INSEE_UNEMPLOYMENT_SOURCE_KEY,
            INSEE_UNEMPLOYMENT_PROGRAM_KEY,
            INSEE_UNEMPLOYMENT_PARSER_ID,
            INSEE_UNEMPLOYMENT_PARSER_VERSION,
        ):
            raise ValueError(
                "INSEE unemployment manifest program metadata differs"
            )
        for value, prefix, name in (
            (self.registry_id, "official-source-registry", "registry_id"),
            (self.source_id, "official-source", "source_id"),
        ):
            if re.fullmatch(rf"{prefix}:sha256:[0-9a-f]{{64}}", value) is None:
                raise ValueError(
                    f"INSEE unemployment manifest {name} is invalid"
                )
        releases = tuple(self.releases)
        if (
            tuple(item.reference_period for item in releases)
            != _release_periods()
        ):
            raise ValueError("INSEE unemployment release coverage differs")
        if tuple(item.source_document_id for item in releases) != (
            self.search.selected_document_ids
        ):
            raise ValueError("INSEE unemployment release/search order differs")
        if tuple(self.coverage_gaps) != INSEE_UNEMPLOYMENT_RELEASE_GAPS:
            raise ValueError("INSEE unemployment source-gap accounting differs")
        if (
            self.catalog_artifact.role
            is not InseeUnemploymentArtifactRole.SERIES_CATALOG
        ):
            raise ValueError("INSEE unemployment catalog artifact role differs")
        expected_comparisons = tuple(
            item.reference_period
            for item in releases
            if item.value.geography_scope == self.current_series.geography_scope
        )
        if (
            tuple(item.reference_period for item in self.comparisons)
            != expected_comparisons
        ):
            raise ValueError(
                "INSEE unemployment exact-scope comparisons differ"
            )
        artifacts = (
            self.search.artifact,
            *(item.artifact for item in releases),
            self.catalog_artifact,
            self.current_series.artifact,
        )
        if self.raw_artifact_count != len(artifacts):
            raise ValueError("INSEE unemployment raw artifact count differs")
        if self.unique_content_sha256_count != len(
            {item.content_sha256 for item in artifacts}
        ):
            raise ValueError("INSEE unemployment unique artifact count differs")
        if self.total_content_bytes != sum(
            item.content_length for item in artifacts
        ):
            raise ValueError("INSEE unemployment total content bytes differ")
        if (
            not 0
            < self.total_content_bytes
            <= MAX_INSEE_UNEMPLOYMENT_TOTAL_BYTES
        ):
            raise ValueError(
                "INSEE unemployment total content bytes are outside bounds"
            )
        if self.released_value_count != len(releases):
            raise ValueError("INSEE unemployment released value count differs")
        if self.compatible_comparison_count != len(self.comparisons):
            raise ValueError("INSEE unemployment comparison count differs")
        if tuple(self.limitations) != _LIMITATIONS:
            raise ValueError("INSEE unemployment limitations differ")

    @property
    def manifest_id(self) -> str:
        return _stable_id(
            "insee-unemployment-archive-manifest", self.identity_payload()
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
    ) -> InseeUnemploymentArchiveManifestV1:
        value = cls(
            registry_id=str(data.get("registry_id", "")),
            source_id=str(data.get("source_id", "")),
            search=InseeUnemploymentSearchV1.from_dict(
                _mapping(data.get("search"), "search")
            ),
            releases=tuple(
                InseeUnemploymentReleaseV1.from_dict(_mapping(item, "release"))
                for item in _sequence(data.get("releases"), "releases")
            ),
            catalog_artifact=InseeUnemploymentArtifactV1.from_dict(
                _mapping(data.get("catalog_artifact"), "catalog_artifact")
            ),
            current_series=InseeUnemploymentCurrentSeriesV1.from_dict(
                _mapping(data.get("current_series"), "current_series")
            ),
            comparisons=tuple(
                InseeUnemploymentComparisonV1.from_dict(
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
            raise ValueError("INSEE unemployment manifest identity differs")
        return value

    @classmethod
    def from_json(cls, payload: str) -> InseeUnemploymentArchiveManifestV1:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(
                "INSEE unemployment manifest JSON is invalid"
            ) from exc
        return cls.from_dict(_mapping(data, "manifest"))


def build_insee_unemployment_archive_manifest(
    registry: OfficialSourceRegistryV1,
    search: InseeUnemploymentSearchV1,
    documents: Sequence[Mapping[str, Any]],
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    catalog_snapshot: OfficialRawSnapshotV1,
    current_series_snapshot: OfficialRawSnapshotV1,
) -> InseeUnemploymentArchiveManifestV1:
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
            "INSEE unemployment retained release inventory differs"
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
        InseeUnemploymentComparisonV1(
            reference_period=item.reference_period,
            release_value=item.value.value,
            current_value=current_by_period[item.reference_period],
            delta=round(
                current_by_period[item.reference_period] - item.value.value, 12
            ),
        )
        for item in releases
        if item.value.geography_scope == current_series.geography_scope
    )
    artifacts = (
        search.artifact,
        *(item.artifact for item in releases),
        catalog_artifact,
        current_series.artifact,
    )
    return InseeUnemploymentArchiveManifestV1(
        registry_id=registry.registry_id,
        source_id=source.source_id,
        search=search,
        releases=releases,
        catalog_artifact=catalog_artifact,
        current_series=current_series,
        comparisons=comparisons,
        coverage_gaps=INSEE_UNEMPLOYMENT_RELEASE_GAPS,
        raw_artifact_count=len(artifacts),
        unique_content_sha256_count=len(
            {item.content_sha256 for item in artifacts}
        ),
        total_content_bytes=sum(item.content_length for item in artifacts),
        released_value_count=len(releases),
        compatible_comparison_count=len(comparisons),
    )


def replay_insee_unemployment_archive(
    registry: OfficialSourceRegistryV1,
    manifest: InseeUnemploymentArchiveManifestV1,
    search_snapshot: OfficialRawSnapshotV1,
    release_snapshots: Mapping[int, OfficialRawSnapshotV1],
    catalog_snapshot: OfficialRawSnapshotV1,
    current_series_snapshot: OfficialRawSnapshotV1,
) -> InseeUnemploymentArchiveManifestV1:
    """Rebuild retained evidence and require byte-independent equality."""
    if registry.registry_id != manifest.registry_id:
        raise ValueError("INSEE unemployment replay registry identity differs")
    search, documents = parse_insee_unemployment_search(
        search_snapshot, as_of_date=manifest.search.as_of_date
    )
    rebuilt = build_insee_unemployment_archive_manifest(
        registry,
        search,
        documents,
        release_snapshots,
        catalog_snapshot,
        current_series_snapshot,
    )
    if rebuilt != manifest:
        raise ValueError(
            "INSEE unemployment replay differs from retained manifest"
        )
    return rebuilt


def packaged_insee_unemployment_manifest_path() -> Path:
    """Return the packaged INSEE quarterly unemployment manifest path."""
    return (
        Path(__file__).with_name("assets")
        / "insee_unemployment_archive_v1.json"
    )


def load_packaged_insee_unemployment_archive_manifest() -> (
    InseeUnemploymentArchiveManifestV1
):
    """Load and fully validate the packaged archive."""
    path = packaged_insee_unemployment_manifest_path()
    if path.stat().st_size > 16 * 1024 * 1024:
        raise ValueError(
            "packaged INSEE unemployment manifest exceeds size bound"
        )
    return InseeUnemploymentArchiveManifestV1.from_json(
        path.read_text(encoding="utf-8")
    )


__all__ = [
    "INSEE_UNEMPLOYMENT_CATALOG_URI",
    "INSEE_UNEMPLOYMENT_CURRENT_SERIES_ID",
    "INSEE_UNEMPLOYMENT_CURRENT_SERIES_URI",
    "INSEE_UNEMPLOYMENT_CURRENT_START_PERIOD",
    "INSEE_UNEMPLOYMENT_LATEST_PACKAGED_PERIOD",
    "INSEE_UNEMPLOYMENT_PARSER_ID",
    "INSEE_UNEMPLOYMENT_PARSER_VERSION",
    "INSEE_UNEMPLOYMENT_PROGRAM_KEY",
    "INSEE_UNEMPLOYMENT_RELEASE_GAPS",
    "INSEE_UNEMPLOYMENT_RELEASE_START_PERIOD",
    "INSEE_UNEMPLOYMENT_SEARCH_URI",
    "INSEE_UNEMPLOYMENT_SOURCE_KEY",
    "InseeUnemploymentArchiveManifestV1",
    "InseeUnemploymentArtifactRole",
    "InseeUnemploymentArtifactV1",
    "InseeUnemploymentComparisonV1",
    "InseeUnemploymentCurrentObservationV1",
    "InseeUnemploymentCurrentSeriesV1",
    "InseeUnemploymentExcludedSearchResultV1",
    "InseeUnemploymentPublishedValueV1",
    "InseeUnemploymentReleaseV1",
    "InseeUnemploymentSearchV1",
    "build_insee_unemployment_archive_manifest",
    "build_insee_unemployment_catalog_request",
    "build_insee_unemployment_current_series_request",
    "build_insee_unemployment_release_requests",
    "build_insee_unemployment_search_request",
    "load_packaged_insee_unemployment_archive_manifest",
    "packaged_insee_unemployment_manifest_path",
    "parse_insee_unemployment_search",
    "replay_insee_unemployment_archive",
]
